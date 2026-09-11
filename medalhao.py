#!/usr/bin/env python3
"""
Pipeline medalhao (bronze -> prata -> ouro) para os extratos CNAB 240 do SFTP.

    bronze/  o arquivo .RET cru, byte a byte, como veio do banco, mais o
             _manifesto.csv que registra o que ja foi ingerido (nome, tamanho,
             data de modificacao na origem e sha256 do conteudo).
    prata/   lancamentos.csv -- um lancamento por linha, no formato do
             DataFrame do cnab240_extrato, com as colunas de controle
             (id_lancamento, hash_arquivo, ingerido_em) e sem duplicatas.
             conferencia.csv -- saldo inicial + movimento = saldo final, por lote.
    ouro/    extrato_lancamentos.csv -- O ARQUIVO FINAL, exatamente no formato
             do DataFrame atual (cnab240_extrato.COLUNAS), ordenado por conta e
             data. conferencia_saldos.csv -- a conferencia consolidada.

Incremental em dois niveis, para nunca reprocessar nem duplicar:

    1. arquivo: a listagem do SFTP e comparada com o manifesto por
       (nome, tamanho, data de modificacao). O que ja esta la nao e baixado.
       Conteudo repetido sob outro nome cai como "duplicado" pelo sha256.
    2. registro: cada lancamento recebe um id_lancamento derivado da chave de
       negocio (banco/agencia/conta + data + valor + D/C + historico +
       documento + ocorrencia). Se o banco reenviar o dia com uma sequencia
       nova, os lancamentos repetidos sao reconhecidos e so entram os novos.

Bronze e a fonte da verdade: `reprocessar` refaz prata e ouro do zero a partir
dos arquivos crus, sem tocar no SFTP -- e o que usar quando o layout do parser
mudar.

Uso
---
    python medalhao.py sftp                      # baixa o que falta e atualiza tudo
    python medalhao.py sftp --path /EXTRATO --recursivo
    python medalhao.py local amostras/*.RET      # ingere arquivos que ja estao no disco
    python medalhao.py reprocessar               # bronze -> prata -> ouro, do zero
    python medalhao.py status                    # o que cada camada tem hoje

Credenciais: SFTP_USER / SFTP_PASSWORD no .env ao lado do script (nunca no codigo).
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterable, Iterator

import pandas as pd

import cnab240_extrato as cnab
import sftp_inventario as inv

LAKE_PADRAO = os.environ.get("LAKE_PATH", "lake")
EXT_PADRAO = (".ret",)

# Colunas de linhagem: existem na prata, nao no arquivo final de ouro.
CONTROLE = ("id_lancamento", "hash_arquivo", "ingerido_em")
COLUNAS_PRATA = list(CONTROLE) + cnab.COLUNAS

COLUNAS_MANIFESTO = [
    "arquivo",
    "origem",
    "tamanho_bytes",
    "mtime_origem",
    "sha256",
    "caminho_bronze",
    "registros",
    "lancamentos",
    "status",
    "ingerido_em",
]

# Chave natural do lancamento. Nao entra o nome do arquivo nem o numero
# sequencial do registro: o mesmo lancamento reenviado em outro arquivo tem de
# gerar o mesmo id. A ocorrencia distingue lancamentos identicos no mesmo dia.
CHAVE_NEGOCIO = (
    "banco",
    "agencia_id",
    "conta_5dig",
    "data_lancamento",
    "valor_centavos",
    "debito_credito",
    "codigo_historico",
    "historico",
    "documento",
    "tipo_complemento",
    "complemento",
)

COLUNAS_NUMERICAS = {
    "n_lote",
    "lote",
    "sequencial",
    "registro",
    "qtd_registros_arquivo",
    "qtd_registros_lote",
    "valor_centavos",
    "valor",
    "valor_assinado",
    "saldo_inicial",
    "saldo_final",
    "total_debitos_lote",
    "total_creditos_lote",
    # conferencia de saldos
    "lancamentos",
    "movimento",
    "esperado",
    "diferenca",
    "apos_data_saldo",
    "valor_apos_data_saldo",
}

ORDEM_OURO = ("banco", "agencia_id", "conta_5dig", "data_lancamento", "n_lote", "sequencial")

COLUNAS_CONFERENCIA = [
    "hash_arquivo",
    "arquivo",
    "lote",
    "lancamentos",
    "saldo_inicial",
    "movimento",
    "esperado",
    "saldo_final",
    "diferenca",
    "apos_data_saldo",
    "valor_apos_data_saldo",
    "ingerido_em",
]


# --------------------------------------------------------------------------- #
# O lago
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Lago:
    """Os caminhos das tres camadas. `formato` vale para prata e ouro."""

    raiz: str = LAKE_PADRAO
    formato: str = "csv"

    @property
    def bronze(self) -> str:
        return os.path.join(self.raiz, "bronze")

    @property
    def prata(self) -> str:
        return os.path.join(self.raiz, "prata")

    @property
    def ouro(self) -> str:
        return os.path.join(self.raiz, "ouro")

    @property
    def manifesto(self) -> str:
        # o manifesto e sempre CSV: precisa ser legivel sem pandas nem pyarrow
        return os.path.join(self.bronze, "_manifesto.csv")

    def tabela(self, camada: str, nome: str) -> str:
        return os.path.join(getattr(self, camada), f"{nome}.{self.formato}")

    def preparar(self) -> None:
        for pasta in (self.bronze, self.prata, self.ouro):
            os.makedirs(pasta, exist_ok=True)


@dataclass(frozen=True)
class Candidato:
    """Um arquivo que pode entrar no bronze. `ler` so e chamado se for novo."""

    nome: str
    origem: str
    tamanho: int
    mtime: str
    ler: Callable[[], bytes]


@dataclass
class Resumo:
    novos: int = 0
    duplicados: int = 0
    ignorados: int = 0
    limitados: int = 0
    erros: int = 0
    lancamentos_novos: int = 0
    arquivos: list[str] = field(default_factory=list)


def agora() -> str:
    return datetime.now(tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def sha256_hex(dados: bytes) -> str:
    return hashlib.sha256(dados).hexdigest()


# --------------------------------------------------------------------------- #
# Leitura e gravacao das tabelas (CSV por padrao, parquet se pedido)
# --------------------------------------------------------------------------- #


def gravar_tabela(df: pd.DataFrame, caminho: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(caminho)), exist_ok=True)
    if caminho.endswith(".parquet"):
        df.to_parquet(caminho, index=False)
    else:
        df.to_csv(caminho, sep=";", index=False, encoding="utf-8-sig")


def ler_tabela(caminho: str, colunas: Iterable[str]) -> pd.DataFrame:
    """Le a tabela preservando os tipos: zeros a esquerda nao podem virar int."""
    colunas = list(colunas)
    if not os.path.isfile(caminho):
        return pd.DataFrame(columns=colunas)
    if caminho.endswith(".parquet"):
        return pd.read_parquet(caminho)
    texto = {
        c: "string"
        for c in colunas
        if c not in COLUNAS_NUMERICAS and c not in cnab.COLUNAS_DATA
    }
    return pd.read_csv(caminho, sep=";", encoding="utf-8-sig", dtype=texto)


def padronizar(df: pd.DataFrame, colunas: Iterable[str]) -> pd.DataFrame:
    """Garante as colunas na ordem certa e com o tipo certo (data, numero, texto)."""
    colunas = list(colunas)
    df = df.copy()
    for coluna in colunas:
        if coluna not in df.columns:
            df[coluna] = pd.NA
    for coluna in colunas:
        if coluna in cnab.COLUNAS_DATA:
            df[coluna] = pd.to_datetime(df[coluna], errors="coerce")
        elif coluna in COLUNAS_NUMERICAS:
            df[coluna] = pd.to_numeric(df[coluna], errors="coerce")
        else:
            df[coluna] = df[coluna].astype("string")
    extras = [c for c in df.columns if c not in colunas]
    return df[colunas + extras]


def ler_manifesto(lago: Lago) -> pd.DataFrame:
    if not os.path.isfile(lago.manifesto):
        return pd.DataFrame(columns=COLUNAS_MANIFESTO)
    texto = {c: "string" for c in COLUNAS_MANIFESTO if c not in ("tamanho_bytes", "registros", "lancamentos")}
    df = pd.read_csv(lago.manifesto, sep=";", encoding="utf-8-sig", dtype=texto)
    for coluna in COLUNAS_MANIFESTO:
        if coluna not in df.columns:
            df[coluna] = pd.NA
    return df[COLUNAS_MANIFESTO]


def gravar_manifesto(lago: Lago, df: pd.DataFrame) -> None:
    os.makedirs(lago.bronze, exist_ok=True)
    df[COLUNAS_MANIFESTO].to_csv(
        lago.manifesto, sep=";", index=False, encoding="utf-8-sig"
    )


# --------------------------------------------------------------------------- #
# Identidade do lancamento
# --------------------------------------------------------------------------- #


def _texto_chave(valor) -> str:
    if valor is None or valor is pd.NA:
        return ""
    if isinstance(valor, float) and pd.isna(valor):
        return ""
    if hasattr(valor, "isoformat"):
        return valor.isoformat()[:10]
    return str(valor).strip()


def id_lancamento(registro: dict, ocorrencia: int) -> str:
    """sha1 da chave de negocio + ocorrencia, truncado em 16 hex."""
    partes = [_texto_chave(registro.get(c)) for c in CHAVE_NEGOCIO]
    if not partes[0]:  # nome fora do padrao: sem banco/agencia/conta no nome
        partes.insert(0, _texto_chave(registro.get("arquivo")))
    partes.append(str(ocorrencia))
    return hashlib.sha1("|".join(partes).encode("utf-8")).hexdigest()[:16]


def com_identidade(registros: list[dict], sha: str, quando: str) -> list[dict]:
    """Anexa id_lancamento e linhagem. A ocorrencia conta repetidos na chave."""
    vistos: dict[str, int] = {}
    saida = []
    for registro in registros:
        base = "|".join(_texto_chave(registro.get(c)) for c in CHAVE_NEGOCIO)
        vistos[base] = vistos.get(base, 0) + 1
        saida.append(
            {
                "id_lancamento": id_lancamento(registro, vistos[base]),
                "hash_arquivo": sha,
                "ingerido_em": quando,
                **registro,
            }
        )
    return saida


# --------------------------------------------------------------------------- #
# Bronze
# --------------------------------------------------------------------------- #


def caminho_bronze(lago: Lago, nome: str, ident: dict, sha: str) -> str:
    """bronze/<banco>/<AAAA-MM>/<arquivo>, com sufixo se o nome ja existir."""
    banco = ident.get("banco") or "sem_banco"
    data = ident.get("data_arquivo") or ""
    competencia = data[:7] if len(data) >= 7 else "sem_data"
    pasta = os.path.join(lago.bronze, banco, competencia)
    os.makedirs(pasta, exist_ok=True)
    destino = os.path.join(pasta, nome)
    if os.path.isfile(destino) and sha256_hex(_ler_arquivo(destino)) != sha:
        raiz, ext = os.path.splitext(nome)
        destino = os.path.join(pasta, f"{raiz}__{sha[:8]}{ext}")
    return destino


def _ler_arquivo(caminho: str) -> bytes:
    with open(caminho, "rb") as f:
        return f.read()


def ingerir(
    lago: Lago,
    candidatos: Iterable[Candidato],
    forcar: bool = False,
    limite: int | None = None,
) -> Resumo:
    """Baixa e grava no bronze so o que ainda nao esta no manifesto."""
    lago.preparar()
    manifesto = ler_manifesto(lago)
    conhecidos = {
        (str(l.arquivo), str(l.tamanho_bytes), str(l.mtime_origem))
        for l in manifesto.itertuples()
    }
    # sha256 -> onde o conteudo ficou no bronze; cresce durante a rodada, para
    # pegar tambem o conteudo repetido que chega duas vezes na mesma varredura
    bronze_por_sha = {
        str(l.sha256): str(l.caminho_bronze)
        for l in manifesto.itertuples()
        if str(l.status) != "duplicado" and not pd.isna(l.sha256)
    }

    resumo = Resumo()
    novas_linhas: list[dict] = []
    novos_registros: list[dict] = []
    novas_conferencias: list[dict] = []
    quando = agora()

    # apontar `local` para o proprio bronze nao ingere nada: o conteudo ja esta
    # la e cairia como duplicado, inflando o manifesto. Quem refaz as camadas a
    # partir do bronze e o `reprocessar`.
    raiz_bronze = os.path.abspath(lago.bronze).replace("\\", "/").lower() + "/"

    for candidato in candidatos:
        if candidato.origem.replace("\\", "/").lower().startswith(raiz_bronze):
            resumo.ignorados += 1
            continue
        assinatura = (candidato.nome, str(candidato.tamanho), str(candidato.mtime))
        if not forcar and assinatura in conhecidos:
            resumo.ignorados += 1
            continue
        if limite is not None and resumo.novos + resumo.duplicados >= limite:
            resumo.limitados += 1
            continue

        try:
            dados = candidato.ler()
        except Exception as exc:  # rede, permissao, arquivo removido no meio
            print(f"[erro] {candidato.origem}: {exc}", file=sys.stderr)
            resumo.erros += 1
            continue

        sha = sha256_hex(dados)
        ident = cnab.parse_nome(candidato.nome)

        if not forcar and sha in bronze_por_sha:
            resumo.duplicados += 1
            novas_linhas.append(
                {
                    "arquivo": candidato.nome,
                    "origem": candidato.origem,
                    "tamanho_bytes": len(dados),
                    "mtime_origem": candidato.mtime,
                    "sha256": sha,
                    "caminho_bronze": bronze_por_sha[sha],
                    "registros": 0,
                    "lancamentos": 0,
                    "status": "duplicado",
                    "ingerido_em": quando,
                }
            )
            conhecidos.add(assinatura)
            print(f"  = {candidato.nome}: conteudo ja ingerido (sha {sha[:8]})")
            continue

        destino = caminho_bronze(lago, candidato.nome, ident, sha)
        with open(destino, "wb") as f:
            f.write(dados)

        linhas = cnab.registros_de_bytes(dados, candidato.nome)
        registros = cnab.lancamentos(candidato.nome, linhas)
        novos_registros.extend(com_identidade(registros, sha, quando))
        for linha in cnab.conferir_saldos(candidato.nome, linhas):
            novas_conferencias.append({"hash_arquivo": sha, **linha, "ingerido_em": quando})

        relativo = os.path.relpath(destino, lago.raiz).replace("\\", "/")
        status = "ok" if registros else ("vazio" if not linhas else "sem_movimento")
        novas_linhas.append(
            {
                "arquivo": candidato.nome,
                "origem": candidato.origem,
                "tamanho_bytes": len(dados),
                "mtime_origem": candidato.mtime,
                "sha256": sha,
                "caminho_bronze": relativo,
                "registros": len(linhas),
                "lancamentos": len(registros),
                "status": status,
                "ingerido_em": quando,
            }
        )
        conhecidos.add(assinatura)
        bronze_por_sha[sha] = relativo
        resumo.novos += 1
        resumo.arquivos.append(candidato.nome)
        print(f"  + {candidato.nome}: {len(linhas)} registro(s), {len(registros)} lancamento(s)")

    if novas_linhas:
        atual = pd.concat([manifesto, pd.DataFrame(novas_linhas)], ignore_index=True)
        gravar_manifesto(lago, atual)

    resumo.lancamentos_novos = promover_prata(lago, novos_registros, novas_conferencias)
    if resumo.novos:  # so o conteudo novo muda a prata -- duplicado nao refaz o ouro
        construir_ouro(lago)
    return resumo


# --------------------------------------------------------------------------- #
# Prata
# --------------------------------------------------------------------------- #


def promover_prata(
    lago: Lago,
    registros: list[dict],
    conferencias: list[dict],
    substituir: bool = False,
) -> int:
    """Anexa os lancamentos novos, descartando ids que a prata ja tem."""
    caminho = lago.tabela("prata", "lancamentos")
    antes = pd.DataFrame(columns=COLUNAS_PRATA) if substituir else ler_tabela(caminho, COLUNAS_PRATA)
    antes = padronizar(antes, COLUNAS_PRATA)

    if registros:
        novos = padronizar(cnab.arrumar(registros), COLUNAS_PRATA)
        conhecidos = set(antes["id_lancamento"].dropna())
        novos = novos[~novos["id_lancamento"].isin(conhecidos)]
        # o mesmo lancamento pode vir duas vezes na mesma rodada (arquivo
        # reenviado com sequencia nova): o primeiro vale
        novos = novos.drop_duplicates(subset="id_lancamento", keep="first")
    else:
        novos = padronizar(pd.DataFrame(columns=COLUNAS_PRATA), COLUNAS_PRATA)

    total = pd.concat([antes, novos], ignore_index=True) if len(novos) else antes
    gravar_tabela(ordenar(total), caminho)
    _prata_conferencia(lago, conferencias, substituir)
    return len(novos)


def _prata_conferencia(lago: Lago, conferencias: list[dict], substituir: bool) -> None:
    caminho = lago.tabela("prata", "conferencia")
    antes = (
        pd.DataFrame(columns=COLUNAS_CONFERENCIA)
        if substituir
        else ler_tabela(caminho, COLUNAS_CONFERENCIA)
    )
    novos = pd.DataFrame(conferencias, columns=COLUNAS_CONFERENCIA) if conferencias else None
    total = pd.concat([antes, novos], ignore_index=True) if novos is not None else antes
    if len(total):
        total = total.drop_duplicates(subset=["hash_arquivo", "lote"], keep="last")
    gravar_tabela(total[COLUNAS_CONFERENCIA], caminho)


def ordenar(df: pd.DataFrame) -> pd.DataFrame:
    chaves = [c for c in ORDEM_OURO if c in df.columns]
    return df.sort_values(chaves, kind="stable").reset_index(drop=True) if chaves else df


# --------------------------------------------------------------------------- #
# Ouro
# --------------------------------------------------------------------------- #


def construir_ouro(lago: Lago) -> pd.DataFrame:
    """O arquivo final: a prata sem as colunas de controle, no formato atual."""
    prata = padronizar(ler_tabela(lago.tabela("prata", "lancamentos"), COLUNAS_PRATA), COLUNAS_PRATA)
    final = ordenar(prata.drop(columns=list(CONTROLE)))
    final = final[[c for c in cnab.COLUNAS if c in final.columns]]
    gravar_tabela(final, lago.tabela("ouro", "extrato_lancamentos"))

    conf = ler_tabela(lago.tabela("prata", "conferencia"), COLUNAS_CONFERENCIA)
    if len(conf):
        conf = conf.drop(columns=["hash_arquivo"]).sort_values(
            [c for c in ("arquivo", "lote") if c in conf.columns], kind="stable"
        )
    gravar_tabela(conf, lago.tabela("ouro", "conferencia_saldos"))
    return final


# --------------------------------------------------------------------------- #
# Origens de arquivos
# --------------------------------------------------------------------------- #


def candidatos_locais(caminhos: Iterable[str]) -> Iterator[Candidato]:
    for caminho in caminhos:
        st = os.stat(caminho)
        yield Candidato(
            nome=os.path.basename(caminho),
            origem=os.path.abspath(caminho).replace("\\", "/"),
            tamanho=st.st_size,
            mtime=inv.formatar_data(st.st_mtime),
            ler=lambda c=caminho: _ler_arquivo(c),
        )


def baixar(sftp, caminho: str) -> bytes:
    with sftp.open(caminho, "rb") as f:
        f.prefetch()
        return f.read()


def candidatos_sftp(
    sftp,
    pasta: str,
    recursivo: bool = False,
    extensoes: tuple[str, ...] | None = EXT_PADRAO,
) -> Iterator[Candidato]:
    for entrada in inv.caminhar(sftp, pasta, recursivo=recursivo):
        if extensoes and entrada.extensao not in extensoes:
            continue
        yield Candidato(
            nome=entrada.nome_arquivo,
            origem=entrada.caminho_completo,
            tamanho=entrada.tamanho_bytes,
            mtime=entrada.data_modificacao,
            ler=lambda c=entrada.caminho_completo: baixar(sftp, c),
        )


def candidatos_bronze(lago: Lago) -> Iterator[Candidato]:
    """Relê o bronze para refazer prata e ouro sem tocar no SFTP."""
    for linha in ler_manifesto(lago).itertuples():
        if str(linha.status) == "duplicado":
            continue
        caminho = os.path.join(lago.raiz, str(linha.caminho_bronze))
        if not os.path.isfile(caminho):
            print(f"[aviso] bronze ausente: {caminho}", file=sys.stderr)
            continue
        yield Candidato(
            nome=str(linha.arquivo),
            origem=str(linha.origem),
            tamanho=int(linha.tamanho_bytes or 0),
            mtime=str(linha.mtime_origem),
            ler=lambda c=caminho: _ler_arquivo(c),
        )


def reprocessar(lago: Lago) -> int:
    """Refaz prata e ouro a partir do bronze. Manifesto e bronze ficam intactos."""
    registros: list[dict] = []
    conferencias: list[dict] = []
    quando = agora()
    total_arquivos = 0

    for candidato in candidatos_bronze(lago):
        dados = candidato.ler()
        sha = sha256_hex(dados)
        linhas = cnab.registros_de_bytes(dados, candidato.nome)
        registros.extend(com_identidade(cnab.lancamentos(candidato.nome, linhas, avisar=False), sha, quando))
        for linha in cnab.conferir_saldos(candidato.nome, linhas):
            conferencias.append({"hash_arquivo": sha, **linha, "ingerido_em": quando})
        total_arquivos += 1

    gravados = promover_prata(lago, registros, conferencias, substituir=True)
    construir_ouro(lago)
    print(
        f"Reprocessados {total_arquivos} arquivo(s) do bronze: "
        f"{len(registros)} lancamento(s) lidos, {gravados} na prata apos deduplicar."
    )
    return 0


# --------------------------------------------------------------------------- #
# Status
# --------------------------------------------------------------------------- #


def status(lago: Lago) -> int:
    print(f"Lago: {os.path.abspath(lago.raiz)} (formato prata/ouro: {lago.formato})")

    manifesto = ler_manifesto(lago)
    print(f"\nBRONZE  {len(manifesto)} arquivo(s) no manifesto")
    if len(manifesto):
        bytes_totais = pd.to_numeric(manifesto["tamanho_bytes"], errors="coerce").sum()
        print(f"        {inv.formatar_tamanho(int(bytes_totais))} de conteudo cru")
        for valor, n in manifesto["status"].value_counts().items():
            print(f"        {valor}: {n}")
        print(f"        ultima ingestao: {manifesto['ingerido_em'].max()}")

    prata = ler_tabela(lago.tabela("prata", "lancamentos"), COLUNAS_PRATA)
    print(f"\nPRATA   {len(prata)} lancamento(s)")
    if len(prata):
        datas = pd.to_datetime(prata["data_lancamento"], errors="coerce")
        contas = prata[["banco", "agencia_id", "conta_5dig"]].drop_duplicates()
        print(f"        periodo: {datas.min():%Y-%m-%d} a {datas.max():%Y-%m-%d}")
        print(f"        {len(contas)} conta(s), {prata['arquivo'].nunique()} arquivo(s) de origem")
        soma = pd.to_numeric(prata["valor_assinado"], errors="coerce").sum()
        print(f"        soma dos lancamentos: {soma:,.2f}")

    caminho_ouro = lago.tabela("ouro", "extrato_lancamentos")
    ouro = ler_tabela(caminho_ouro, cnab.COLUNAS)
    print(f"\nOURO    {len(ouro)} linha(s) x {len(ouro.columns)} coluna(s)")
    print(f"        {os.path.abspath(caminho_ouro)}")

    conf = ler_tabela(lago.tabela("ouro", "conferencia_saldos"), COLUNAS_CONFERENCIA)
    if len(conf):
        dif = pd.to_numeric(conf["diferenca"], errors="coerce")
        fecham = int((dif == 0).sum())
        print(f"        conferencia: {fecham}/{len(conf)} lote(s) fecham")
    return 0


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _comuns(p: argparse.ArgumentParser) -> None:
    p.add_argument("--lake", default=LAKE_PADRAO, help=f"Raiz do lago (padrao: {LAKE_PADRAO}).")
    p.add_argument(
        "--formato",
        choices=("csv", "parquet"),
        default=os.environ.get("LAKE_FORMATO", "csv"),
        help="Formato de prata e ouro (parquet exige pyarrow; padrao: csv).",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Pipeline medalhao dos extratos CNAB 240 (bronze -> prata -> ouro).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Uso\n---\n")[-1],
    )
    # sem subcomando (o botao Run da IDE nao passa argumento) cai no status,
    # que so le o lago: nao conecta em nada nem grava nada
    sub = p.add_subparsers(dest="comando", required=False)

    s = sub.add_parser("sftp", help="Baixa do SFTP o que ainda nao esta no bronze.")
    _comuns(s)
    s.add_argument("--host", default=os.environ.get("SFTP_HOST", inv.DEFAULT_HOST))
    s.add_argument("--port", type=int, default=int(os.environ.get("SFTP_PORT", inv.DEFAULT_PORT)))
    s.add_argument("--user", default=os.environ.get("SFTP_USER"))
    s.add_argument("--key", help="Chave privada, no lugar da senha.")
    s.add_argument("--timeout", type=float, default=30.0)
    s.add_argument("--path", default=inv.PASTA_PADRAO, help=f"Pasta (padrao: {inv.PASTA_PADRAO}).")
    s.add_argument("--recursivo", action="store_true", help="Desce nas subpastas.")
    s.add_argument(
        "--ext",
        nargs="+",
        metavar="EXT",
        default=list(EXT_PADRAO),
        help=f"Extensoes ingeridas (padrao: {' '.join(EXT_PADRAO)}; use --ext '' para todas).",
    )
    s.add_argument("--limite", type=int, help="Ingere no maximo N arquivos novos nesta rodada.")
    s.add_argument("--forcar", action="store_true", help="Reingere mesmo o que ja esta no manifesto.")

    l = sub.add_parser("local", help="Ingere arquivos .RET que ja estao no disco.")
    _comuns(l)
    l.add_argument("arquivos", nargs="+", help="Caminhos locais (aceita curinga).")
    l.add_argument("--limite", type=int)
    l.add_argument("--forcar", action="store_true")

    r = sub.add_parser("reprocessar", help="Refaz prata e ouro a partir do bronze.")
    _comuns(r)

    t = sub.add_parser("status", help="Mostra o que cada camada tem hoje.")
    _comuns(t)

    return p.parse_args(argv)


def imprimir_resumo(resumo: Resumo, lago: Lago) -> None:
    print(
        f"\nBronze: {resumo.novos} novo(s), {resumo.duplicados} duplicado(s), "
        f"{resumo.ignorados} ja ingerido(s), {resumo.erros} com erro."
        + (f" {resumo.limitados} fora do --limite." if resumo.limitados else "")
    )
    print(f"Prata:  {resumo.lancamentos_novos} lancamento(s) novo(s) apos deduplicar.")
    if resumo.novos:
        print(f"Ouro:   {os.path.abspath(lago.tabela('ouro', 'extrato_lancamentos'))}")
    else:
        print("Ouro:   nada a refazer -- nenhum arquivo novo.")


def main(argv: list[str] | None = None) -> int:
    inv.carregar_env(inv.ENV_FILES)
    args = parse_args(argv)
    lago = Lago(raiz=args.lake, formato=args.formato)

    if args.comando == "status":
        return status(lago)
    if args.comando == "reprocessar":
        return reprocessar(lago)

    if args.comando == "local":
        caminhos: list[str] = []
        for padrao in args.arquivos:
            caminhos.extend(sorted(glob.glob(padrao, recursive=True)) or [padrao])
        faltando = [c for c in caminhos if not os.path.isfile(c)]
        if faltando:
            print(f"Erro: arquivo(s) nao encontrado(s): {faltando}", file=sys.stderr)
            return 2
        print(f"Ingerindo {len(caminhos)} arquivo(s) local(is):")
        resumo = ingerir(lago, candidatos_locais(caminhos), args.forcar, args.limite)
        imprimir_resumo(resumo, lago)
        return 0

    # comando sftp
    try:
        sftp, transport = inv.abrir_sftp(args.host, args.port, args.user, args.key, args.timeout)
    except Exception as exc:
        print(f"Erro ao conectar: {exc}", file=sys.stderr)
        return 1

    try:
        pasta = inv.resolver_pasta(sftp, args.path)
        if pasta is None:
            print(f"Erro: pasta {args.path} nao encontrada no servidor.", file=sys.stderr)
            return 1
        extensoes = inv.normalizar_ext(args.ext) if any(args.ext) else None
        print(f"Varrendo {pasta} em {args.host} ...")
        resumo = ingerir(
            lago,
            candidatos_sftp(sftp, pasta, args.recursivo, tuple(extensoes) if extensoes else None),
            args.forcar,
            args.limite,
        )
    finally:
        sftp.close()
        transport.close()

    imprimir_resumo(resumo, lago)
    return 0


if __name__ == "__main__":
    sys.exit(main())
