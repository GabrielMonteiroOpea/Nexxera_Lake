#!/usr/bin/env python3
"""
Le um extrato bancario CNAB 240 (.RET) e devolve um DataFrame de lancamentos.

Cada arquivo do SFTP e o extrato de UMA conta, no formato:

    registro 0  header de arquivo
    registro 1  header de lote      (saldo inicial)
    registro 3  detalhe segmento E  (um por lancamento)  <- linhas do DataFrame
    registro 5  trailer de lote     (saldos)
    registro 9  trailer de arquivo

Arquivos de 4 registros (968 bytes) nao tem lancamento nenhum.

A identificacao de agencia e conta sai do NOME do arquivo, no padrao
ext_<banco>_<agencia>_<conta>_<data>[_<seq>].RET, e e conferida contra o
conteudo do registro -- se as posicoes do layout nao casarem com o nome, o
parser avisa e mostra onde os valores realmente estao.

Uso
---
    python cnab240_extrato.py amostras/ext_341_0350_46575_01092600.RET
    python cnab240_extrato.py amostras/*.RET --saida lancamentos.csv
    python cnab240_extrato.py amostras/arquivo.RET --posicoes   # mapa do layout
"""

from __future__ import annotations

import argparse
import glob
import os
import re
import sys
from datetime import datetime

import pandas as pd

LARGURA = 240

# Nome do arquivo: ext_<banco 3>_<agencia 4-5>_<conta 5-13>_<data e sequencia>
RE_NOME = re.compile(
    r"^ext_(?P<banco>\d{3})_(?P<agencia>\d{4,5})_(?P<conta>\d{5,13})_(?P<cauda>[\d_]+)$",
    re.IGNORECASE,
)

# --------------------------------------------------------------------------- #
# Layout do segmento E
#
# Bloco de identificacao (posicoes 1 a 102): estavel no padrao FEBRABAN e
# conferido automaticamente contra o nome do arquivo por conferir_layout().
# --------------------------------------------------------------------------- #
IDENTIFICACAO = (
    # (campo, posicao inicial 1-based, tamanho, tipo)
    ("banco_reg", 1, 3, "txt"),
    ("lote", 4, 4, "num"),
    ("registro", 8, 1, "num"),
    ("sequencial", 9, 5, "num"),
    ("segmento", 14, 1, "txt"),
    ("tipo_inscricao", 18, 1, "txt"),
    ("inscricao", 19, 14, "txt"),
    ("convenio", 33, 20, "txt"),
    ("agencia_reg", 53, 5, "txt"),
    ("agencia_dv", 58, 1, "txt"),
    ("conta_reg", 59, 12, "txt"),
    ("conta_dv", 71, 1, "txt"),
    ("dv_agencia_conta", 72, 1, "txt"),
    ("nome_empresa", 73, 30, "txt"),
)

# Bloco de movimentacao (posicoes 103 a 240), medido nos arquivos reais da pasta
# EXTRATO: posicoes identicas nos bancos 341 (Itau) e 033 (Santander).
#   103-108 brancos
#   109-111 DPV (Itau) / CDS (Santander)
#   134     S/N
#   135-142 data contabil -- o Santander preenche, o Itau deixa em branco
#   143-150 data do lancamento (DDMMAAAA), sempre preenchida
#   151-168 valor, 18 digitos com 2 decimais
#   169     D/C
#   170-176 codigo do historico do banco (o mesmo lancamento repete o codigo:
#           0098 = SISPAG, 0045 = aplicacao automatica, ...)
#   177-201 historico (texto)
#   202-240 documento / complemento livre
MOVIMENTACAO = (
    ("tipo_complemento", 109, 3, "txt"),
    ("complemento", 112, 22, "txt"),
    ("situacao", 134, 1, "txt"),
    ("data_contabil", 135, 8, "data"),
    ("data_lancamento", 143, 8, "data"),
    ("valor", 151, 18, "valor"),
    ("debito_credito", 169, 1, "txt"),
    ("codigo_historico", 170, 7, "txt"),
    ("historico", 177, 25, "txt"),
    ("documento", 202, 39, "txt"),
)

# Header (tipo 1) e trailer (tipo 5) de lote carregam os saldos, nas mesmas
# posicoes de data/valor/natureza do segmento E. Um arquivo pode ter mais de um
# lote (um por tipo de conta), e ha lotes sem nenhum lancamento.
HEADER_LOTE = (
    ("data_saldo", 143, 8, "data"),
    ("valor_saldo", 151, 18, "valor"),
    ("natureza_saldo", 169, 1, "txt"),
    ("situacao_saldo", 170, 1, "txt"),
    ("moeda", 171, 3, "txt"),
)
TRAILER_LOTE = (
    ("data_saldo", 143, 8, "data"),
    ("valor_saldo", 151, 18, "valor"),
    ("natureza_saldo", 169, 1, "txt"),
    ("situacao_saldo", 170, 1, "txt"),
    ("qtd_registros_lote", 171, 6, "num"),
    ("total_debitos", 177, 18, "valor"),
    ("total_creditos", 195, 18, "valor"),
)

SEGMENTO_E = IDENTIFICACAO + MOVIMENTACAO


# --------------------------------------------------------------------------- #
# Nome do arquivo
# --------------------------------------------------------------------------- #


def parse_data_cauda(cauda: str) -> tuple[str | None, str]:
    """A cauda do nome traz data + sequencia, juntas ou separadas por '_'.

    10 digitos -> AAAAMMDD + seq (ex.: 2026082200)
     8 digitos -> AAAAMMDD (ex.: 20260902) ou DDMMAA + seq (ex.: 01092600)
     6 digitos -> DDMMAA
    """
    digitos = cauda.replace("_", "")
    if len(digitos) == 10:
        return digitos[:8], digitos[8:]
    if len(digitos) == 8:
        if digitos[:2] in ("19", "20") and 1 <= int(digitos[4:6]) <= 12:
            return digitos, ""
        return digitos[:6], digitos[6:]
    if len(digitos) == 6:
        return digitos, ""
    return None, digitos


def formatar_data_nome(bruta: str | None) -> str:
    if not bruta:
        return ""
    formato = {8: "%Y%m%d", 6: "%d%m%y"}.get(len(bruta))
    if not formato:
        return bruta
    try:
        return datetime.strptime(bruta, formato).date().isoformat()
    except ValueError:
        return bruta


def parse_nome(nome_arquivo: str) -> dict:
    """Extrai banco / agencia / conta / data do nome do arquivo."""
    base = os.path.basename(nome_arquivo)
    sem_ext = os.path.splitext(base)[0]
    m = RE_NOME.match(sem_ext)
    if not m:
        return {
            "arquivo": base,
            "banco": "",
            "agencia": "",
            "agencia_id": "",
            "conta": "",
            "conta_5dig": "",
            "data_arquivo": "",
            "sequencia": "",
        }

    agencia = m.group("agencia")
    conta = m.group("conta")
    data_bruta, seq = parse_data_cauda(m.group("cauda"))
    return {
        "arquivo": base,
        "banco": m.group("banco"),
        "agencia": agencia,
        # chave de negocio: agencia sem zeros a esquerda (3 digitos no Itau:
        # 0350 -> 350, 0910 -> 910) e os ultimos 5 digitos da conta.
        # Nao truncar: a agencia 02271 do Santander tem 4 digitos (2271).
        "agencia_id": agencia.lstrip("0").zfill(3),
        "conta": conta,
        "conta_5dig": conta[-5:],
        "data_arquivo": formatar_data_nome(data_bruta),
        "sequencia": seq,
    }


# --------------------------------------------------------------------------- #
# Leitura dos registros
# --------------------------------------------------------------------------- #


def ler_registros(caminho: str) -> list[str]:
    """Devolve as linhas de 240 caracteres do arquivo."""
    with open(caminho, "rb") as f:
        return registros_de_bytes(f.read(), os.path.basename(caminho))


def registros_de_bytes(bruto: bytes, rotulo: str = "") -> list[str]:
    """Mesma leitura de ler_registros, a partir do conteudo em memoria."""
    texto = bruto.decode("latin-1")
    linhas = [ln for ln in texto.splitlines() if ln.strip()]

    if not linhas:
        return []

    # alguns geradores nao usam quebra de linha: o arquivo e um bloco continuo
    if len(linhas) == 1 and len(linhas[0]) > LARGURA:
        bloco = linhas[0]
        linhas = [bloco[i : i + LARGURA] for i in range(0, len(bloco), LARGURA)]

    fora = [(i, len(ln)) for i, ln in enumerate(linhas, 1) if len(ln) != LARGURA]
    if fora:
        print(
            f"[aviso] {rotulo}: {len(fora)} registro(s) com largura "
            f"diferente de {LARGURA}: {fora[:5]}",
            file=sys.stderr,
        )
    return linhas


def tipo_registro(linha: str) -> str:
    return linha[7:8]


def segmento(linha: str) -> str:
    return linha[13:14] if tipo_registro(linha) == "3" else ""


def converter(valor: str, tipo: str):
    valor = valor.strip()
    if tipo == "txt":
        return valor
    if tipo == "num":
        return int(valor) if valor.isdigit() else None
    if tipo == "valor":
        return int(valor) if valor.isdigit() else None
    if tipo == "data":
        if not valor.isdigit() or len(valor) != 8 or valor == "0" * 8:
            return None
        for formato in ("%d%m%Y", "%Y%m%d"):
            try:
                return datetime.strptime(valor, formato).date()
            except ValueError:
                continue
        return None
    return valor


def fatiar(linha: str, campos) -> dict:
    dados = {}
    for nome, ini, tam, tipo in campos:
        bruto = linha[ini - 1 : ini - 1 + tam]
        dados[nome] = converter(bruto, tipo)
    return dados


# --------------------------------------------------------------------------- #
# Conferencia do layout contra o nome do arquivo
# --------------------------------------------------------------------------- #


def conferir_layout(linha: str, ident: dict) -> list[str]:
    """Compara banco/agencia/conta do registro com o nome do arquivo."""
    avisos = []
    checagens = (
        ("banco", 1, 3, ident["banco"]),
        ("agencia", 53, 5, ident["agencia"]),
        ("conta", 59, 12, ident["conta"]),
    )
    for nome, ini, tam, esperado in checagens:
        if not esperado:
            continue
        no_registro = linha[ini - 1 : ini - 1 + tam].strip().lstrip("0")
        referencia = esperado.lstrip("0")
        # o nome do arquivo pode ou nao concatenar o DV da conta, e o DV fica na
        # posicao 71 (Santander 02271) ou na 72 (Itau 00910); a agencia 0350 do
        # Itau nao traz DV no nome. Aceitar as tres convencoes.
        aceitos = {no_registro}
        if nome == "conta":
            aceitos |= {no_registro + linha[70:71].strip(), no_registro + linha[71:72].strip()}
        if referencia in aceitos:
            continue
        onde = localizar(linha, esperado)
        avisos.append(
            f"{nome}: nome do arquivo diz {esperado!r}, posicao {ini}-{ini + tam - 1} "
            f"do registro tem {no_registro!r}"
            + (f"; o valor aparece na posicao {onde}" if onde else "; nao achei no registro")
        )
    return avisos


def localizar(linha: str, valor: str) -> str:
    """Procura o valor (com e sem zeros a esquerda) dentro do registro."""
    for alvo in {valor, valor.lstrip("0"), valor.zfill(5), valor.zfill(12)}:
        if not alvo:
            continue
        pos = linha.find(alvo)
        if pos >= 0:
            return f"{pos + 1}-{pos + len(alvo)}"
    return ""


# --------------------------------------------------------------------------- #
# Descoberta de posicoes (para calibrar o bloco de movimentacao)
# --------------------------------------------------------------------------- #


def candidatos_data(linha: str) -> list[str]:
    achados = []
    for i in range(len(linha) - 7):
        trecho = linha[i : i + 8]
        if not trecho.isdigit() or trecho == "0" * 8:
            continue
        for formato, rotulo in (("%d%m%Y", "DDMMAAAA"), ("%Y%m%d", "AAAAMMDD")):
            try:
                d = datetime.strptime(trecho, formato).date()
            except ValueError:
                continue
            if 2000 <= d.year <= 2100:
                achados.append(f"{i + 1}-{i + 8}={trecho} ({rotulo} -> {d.isoformat()})")
    return achados


def candidatos_valor(linha: str) -> list[str]:
    """Blocos longos de digitos que podem ser valores com 2 decimais."""
    achados = []
    for m in re.finditer(r"\d{10,}", linha):
        bruto = m.group()
        if set(bruto) == {"0"}:
            continue
        centavos = int(bruto)
        achados.append(
            f"{m.start() + 1}-{m.end()} len={len(bruto)} {bruto} -> {centavos / 100:,.2f}"
        )
    return achados


def candidatos_dc(linha: str) -> list[str]:
    achados = []
    for i, ch in enumerate(linha):
        if ch in "DC":
            antes = linha[i - 1] if i else " "
            depois = linha[i + 1] if i + 1 < len(linha) else " "
            if not antes.isalpha() and not depois.isalpha():
                achados.append(f"{i + 1}={ch}")
    return achados


def mostrar_posicoes(caminho: str) -> None:
    ident = parse_nome(caminho)
    linhas = ler_registros(caminho)
    print("=" * 78)
    print(f"ARQUIVO: {ident['arquivo']}")
    print(
        f"Nome: banco={ident['banco']} agencia={ident['agencia']} "
        f"({ident['agencia_id']}) conta={ident['conta']} ({ident['conta_5dig']}) "
        f"data={ident['data_arquivo']} seq={ident['sequencia']}"
    )
    print(f"Registros: {len(linhas)}")
    tipos = [(i, tipo_registro(ln), segmento(ln)) for i, ln in enumerate(linhas, 1)]
    print("Estrutura: " + " ".join(f"{t}{s}" for _, t, s in tipos))
    print()

    for i, linha in enumerate(linhas, 1):
        tipo, seg = tipo_registro(linha), segmento(linha)
        print(f"--- registro {i}: tipo {tipo}{' segmento ' + seg if seg else ''}")
        print(regua_com_linha(linha))
        if tipo == "3" and seg == "E":
            for aviso in conferir_layout(linha, ident):
                print(f"  [conferir] {aviso}")
            print("  datas encontradas:")
            for c in candidatos_data(linha) or ["    (nenhuma)"]:
                print(f"    {c}")
            print("  possiveis valores:")
            for c in candidatos_valor(linha)[:12] or ["    (nenhum)"]:
                print(f"    {c}")
            print(f"  possiveis D/C: {', '.join(candidatos_dc(linha)) or '(nenhum)'}")
            print("  mapeamento atual:")
            for nome, valor in fatiar(linha, SEGMENTO_E).items():
                print(f"    {nome:<18} = {valor!r}")
        print()


def regua_com_linha(linha: str, bloco: int = 60) -> str:
    partes = []
    for ini in range(0, len(linha), bloco):
        trecho = linha[ini : ini + bloco]
        cabeca = "".join(
            str(((ini + i) // 10 + 1) % 10) if (ini + i) % 10 == 9 else " "
            for i in range(len(trecho))
        )
        partes.append(f"     {cabeca}\n{ini + 1:>4} {trecho}")
    return "\n".join(partes)


# --------------------------------------------------------------------------- #
# DataFrame
# --------------------------------------------------------------------------- #


def lancamentos_do_arquivo(caminho: str, avisar: bool = True) -> list[dict]:
    return lancamentos(caminho, ler_registros(caminho), avisar=avisar)


def lancamentos_de_bytes(nome: str, bruto: bytes, avisar: bool = True) -> list[dict]:
    """Ponto de entrada para quem ja tem o conteudo em memoria (ex.: SFTP)."""
    return lancamentos(nome, registros_de_bytes(bruto, nome), avisar=avisar)


def percorrer_lotes(linhas: list[str]) -> list[dict]:
    """Agrupa os registros em lotes: header (1), detalhes (3) e trailer (5)."""
    lotes: list[dict] = []
    atual: dict | None = None
    for linha in linhas:
        tipo = tipo_registro(linha)
        if tipo == "1":
            atual = {"header": linha, "detalhes": [], "trailer": None}
            lotes.append(atual)
        elif tipo == "3" and atual is not None:
            atual["detalhes"].append(linha)
        elif tipo == "5" and atual is not None:
            atual["trailer"] = linha
            atual = None
    return lotes


def saldo(linha: str | None, campos) -> dict:
    """Le o saldo de um header/trailer de lote, com o sinal da natureza C/D."""
    if linha is None:
        return {}
    dados = fatiar(linha, campos)
    for nome, _, _, tipo in campos:
        if tipo == "valor" and dados.get(nome) is not None:
            dados[nome] = dados[nome] / 100
    if dados.get("natureza_saldo") == "D" and dados.get("valor_saldo") is not None:
        dados["valor_saldo"] = -dados["valor_saldo"]
    return dados


def lancamentos(nome_arquivo: str, linhas: list[str], avisar: bool = True) -> list[dict]:
    ident = parse_nome(nome_arquivo)
    if not ident["banco"] and avisar:
        print(
            f"[aviso] nome fora do padrao ext_banco_agencia_conta_data: {ident['arquivo']}",
            file=sys.stderr,
        )

    lotes = percorrer_lotes(linhas)
    primeiro_e = next(
        (
            ln
            for lote in lotes
            for ln in lote["detalhes"]
            if segmento(ln) == "E"
        ),
        None,
    )
    if avisar and primeiro_e is not None:
        for aviso in conferir_layout(primeiro_e, ident):
            print(f"[conferir] {ident['arquivo']}: {aviso}", file=sys.stderr)

    registros = []
    for n_lote, lote in enumerate(lotes, 1):
        inicial = saldo(lote["header"], HEADER_LOTE)
        final = saldo(lote["trailer"], TRAILER_LOTE)
        contexto = {
            "n_lote": n_lote,
            "moeda": inicial.get("moeda", ""),
            "data_saldo_inicial": inicial.get("data_saldo"),
            "saldo_inicial": inicial.get("valor_saldo"),
            "data_saldo_final": final.get("data_saldo"),
            "saldo_final": final.get("valor_saldo"),
            "total_debitos_lote": final.get("total_debitos"),
            "total_creditos_lote": final.get("total_creditos"),
            "qtd_registros_lote": final.get("qtd_registros_lote"),
        }
        for linha in lote["detalhes"]:
            if segmento(linha) != "E":
                continue
            campos = fatiar(linha, SEGMENTO_E)
            centavos = campos.pop("valor", None)
            dc = (campos.get("debito_credito") or "").upper()
            valor = None if centavos is None else centavos / 100
            registros.append(
                {
                    **ident,
                    "conta_sem_dv": (campos.get("conta_reg") or "").lstrip("0"),
                    "qtd_registros_arquivo": len(linhas),
                    **contexto,
                    **campos,
                    "valor_centavos": centavos,
                    "valor": valor,
                    "valor_assinado": None if valor is None else (-valor if dc == "D" else valor),
                }
            )
    return registros


def conferir_saldos(nome_arquivo: str, linhas: list[str]) -> list[dict]:
    """saldo inicial + soma dos lancamentos deve fechar com o saldo final."""
    resultado = []
    for n_lote, lote in enumerate(percorrer_lotes(linhas), 1):
        inicial = saldo(lote["header"], HEADER_LOTE).get("valor_saldo")
        final = saldo(lote["trailer"], TRAILER_LOTE).get("valor_saldo")
        dados_final = saldo(lote["trailer"], TRAILER_LOTE)
        data_corte = dados_final.get("data_saldo")
        movimento = posteriores = 0.0
        n_posteriores = 0
        for linha in lote["detalhes"]:
            if segmento(linha) != "E":
                continue
            campos = fatiar(linha, SEGMENTO_E)
            centavos = campos.get("valor")
            if centavos is None:
                continue
            reais = centavos / 100
            assinado = -reais if campos.get("debito_credito") == "D" else reais
            data_lanc = campos.get("data_lancamento")
            if data_corte and data_lanc and data_lanc > data_corte:
                posteriores += assinado
                n_posteriores += 1
            else:
                movimento += assinado
        esperado = None if inicial is None else round(inicial + movimento, 2)
        resultado.append(
            {
                "arquivo": os.path.basename(nome_arquivo),
                "lote": n_lote,
                "lancamentos": sum(1 for l in lote["detalhes"] if segmento(l) == "E"),
                "saldo_inicial": inicial,
                "movimento": round(movimento, 2),
                "esperado": esperado,
                "saldo_final": final,
                "diferenca": None
                if esperado is None or final is None
                else round(final - esperado, 2),
                "apos_data_saldo": n_posteriores,
                "valor_apos_data_saldo": round(posteriores, 2),
            }
        )
    return resultado


COLUNAS = [
    "arquivo",
    "banco",
    "agencia",
    "agencia_id",
    "conta",
    "conta_5dig",
    "conta_sem_dv",
    "data_arquivo",
    "sequencia",
    "n_lote",
    "moeda",
    "data_saldo_inicial",
    "saldo_inicial",
    "data_saldo_final",
    "saldo_final",
    "total_debitos_lote",
    "total_creditos_lote",
    "qtd_registros_lote",
    "lote",
    "sequencial",
    "segmento",
    "data_contabil",
    "data_lancamento",
    "tipo_complemento",
    "complemento",
    "codigo_historico",
    "historico",
    "documento",
    "situacao",
    "debito_credito",
    "valor_centavos",
    "valor",
    "valor_assinado",
    "nome_empresa",
    "tipo_inscricao",
    "inscricao",
    "convenio",
    "agencia_reg",
    "agencia_dv",
    "conta_reg",
    "conta_dv",
    "dv_agencia_conta",
    "banco_reg",
    "registro",
    "qtd_registros_arquivo",
]


COLUNAS_DATA = (
    "data_arquivo",
    "data_saldo_inicial",
    "data_saldo_final",
    "data_contabil",
    "data_lancamento",
)


def arrumar(registros: list[dict]) -> pd.DataFrame:
    """Ordena as colunas e converte as datas para datetime64."""
    if not registros:
        return pd.DataFrame(columns=COLUNAS)
    df = pd.DataFrame(registros)
    for coluna in COLUNAS_DATA:
        if coluna in df.columns:
            df[coluna] = pd.to_datetime(df[coluna], errors="coerce")
    ordenadas = [c for c in COLUNAS if c in df.columns]
    return df[ordenadas + [c for c in df.columns if c not in ordenadas]]


def montar_dataframe(caminhos: list[str], avisar: bool = True) -> pd.DataFrame:
    registros: list[dict] = []
    for caminho in caminhos:
        registros.extend(lancamentos_do_arquivo(caminho, avisar=avisar))
    return arrumar(registros)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Le extratos CNAB 240 (.RET) e monta um DataFrame de lancamentos.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("arquivos", nargs="+", help="Arquivos .RET locais (aceita curinga).")
    p.add_argument(
        "--posicoes",
        action="store_true",
        help="Mostra o mapa do layout de cada registro, para calibrar as posicoes.",
    )
    p.add_argument(
        "--conferir",
        action="store_true",
        help="Confere, por lote: saldo inicial + lancamentos = saldo final.",
    )
    p.add_argument("--saida", help="Grava o DataFrame neste CSV (delimitador ';').")
    p.add_argument("--linhas", type=int, default=20, help="Linhas exibidas (padrao: 20).")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    caminhos: list[str] = []
    for padrao in args.arquivos:
        achados = sorted(glob.glob(padrao, recursive=True))  # recursive: aceita '**'
        caminhos.extend(achados or [padrao])
    faltando = [c for c in caminhos if not os.path.isfile(c)]
    if faltando:
        print(f"Erro: arquivo(s) nao encontrado(s): {faltando}", file=sys.stderr)
        return 2

    if args.posicoes:
        for caminho in caminhos:
            mostrar_posicoes(caminho)
        return 0

    if args.conferir:
        checagens = []
        for caminho in caminhos:
            checagens.extend(conferir_saldos(caminho, ler_registros(caminho)))
        conf = pd.DataFrame(checagens)
        with pd.option_context("display.width", 200, "display.max_columns", 20):
            print(conf.to_string(index=False))
        fecham = int((conf["diferenca"] == 0).sum())
        print(f"{fecham}/{len(conf)} lote(s) fecham; " f"{len(conf) - fecham} com diferenca.")
        return 0 if fecham == len(conf) else 1

    df = montar_dataframe(caminhos)
    if df.empty:
        print(
            f"{len(caminhos)} arquivo(s) lido(s), nenhum lancamento (segmento E) encontrado.\n"
            "Extratos de 4 registros nao tem movimento; rode com --posicoes para ver a estrutura."
        )
        return 0

    with pd.option_context("display.width", 200, "display.max_columns", 40):
        print(df.head(args.linhas).to_string(index=False))
    print(f"\n{len(df)} lancamento(s) de {len(caminhos)} arquivo(s).")
    print(f"Colunas: {len(df.columns)}")
    if "valor_assinado" in df and df["valor_assinado"].notna().any():
        print(f"Soma dos lancamentos: {df['valor_assinado'].sum():,.2f}")
    if args.saida:
        df.to_csv(args.saida, sep=";", index=False, encoding="utf-8-sig")
        print(f"CSV: {os.path.abspath(args.saida)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
