#!/usr/bin/env python3
"""
Lista a pasta EXTRATO de um servidor SFTP e interpreta o formato dos arquivos.

Por padrao lista apenas a pasta informada em --path (padrao /EXTRATO), sem descer
em subpastas -- use --recursivo para varrer a arvore inteira. Com --amostra N (ou
--arquivo CAMINHO) baixa os primeiros bytes e deduz o layout: encoding, quebra de
linha, delimitador ou largura fixa, numero de colunas, cabecalho e CNAB 240/400.

Credenciais vem do arquivo .env ao lado do script (SFTP_USER, SFTP_PASSWORD) ou
das variaveis de ambiente equivalentes. Nunca ficam no codigo.

Exemplos
--------
    # lista /EXTRATO e mostra o layout do primeiro arquivo
    python sftp_inventario.py --amostra 1 --sem-csv

    # analisa um arquivo especifico
    python sftp_inventario.py --arquivo /EXTRATO/EXTRATO_20260901.RET

    # inventario recursivo do servidor inteiro, gravando CSV
    python sftp_inventario.py --path / --recursivo
"""

from __future__ import annotations

import argparse
import csv
import os
import stat
import sys
from collections import Counter
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from getpass import getpass
from typing import Iterator

import paramiko

DEFAULT_HOST = "sftp-drive.opea.solutions"
DEFAULT_PORT = 22
PREVIEW_LIMIT = 200
EXTENSOES_SUGERIDAS = (".csv", ".xlsx", ".pdf")
_AQUI = os.path.dirname(os.path.abspath(__file__))
ENV_FILES = (
    [os.environ["SFTP_ENV_FILE"]]
    if os.environ.get("SFTP_ENV_FILE")
    else [os.path.join(_AQUI, ".env"), os.path.join(_AQUI, "pwd.env")]
)
PASTA_PADRAO = os.environ.get("SFTP_PATH", "/EXTRATO")
AMOSTRA_BYTES = 16 * 1024
LINHAS_EXIBIDAS = 8
COLUNAS_EXIBIDAS = 160

# Assinaturas de arquivos binários: não adianta tentar interpretar como texto.
MAGIC_BINARIO = (
    (b"PK\x03\x04", "ZIP / XLSX / DOCX"),
    (b"%PDF", "PDF"),
    (b"\xd0\xcf\x11\xe0", "XLS antigo (OLE2)"),
    (b"\x1f\x8b", "GZIP"),
    (b"Rar!", "RAR"),
    (b"SQLite format 3", "SQLite"),
)
DELIMITADORES = ((";", "ponto e vírgula"), ("\t", "tab"), ("|", "pipe"), (",", "vírgula"))


@dataclass(frozen=True)
class Entrada:
    """Uma linha do inventário."""

    caminho_completo: str
    nome_arquivo: str
    extensao: str
    tamanho_bytes: int
    tamanho_legivel: str
    data_modificacao: str


def carregar_env(caminhos: list[str]) -> None:
    """Lê .env simples (CHAVE=valor) sem sobrescrever variáveis já definidas."""
    for caminho in caminhos:
        _carregar_um_env(caminho)


def _carregar_um_env(caminho: str) -> None:
    if not os.path.isfile(caminho):
        return
    with open(caminho, encoding="utf-8-sig") as f:
        for linha in f:
            linha = linha.strip()
            if not linha or linha.startswith("#") or "=" not in linha:
                continue
            chave, _, valor = linha.partition("=")
            chave = chave.strip()
            valor = valor.strip().strip('"').strip("'")
            if chave and chave not in os.environ:
                os.environ[chave] = valor


def formatar_tamanho(n: int) -> str:
    unidades = ("B", "KB", "MB", "GB", "TB", "PB")
    valor = float(n)
    for unidade in unidades:
        if valor < 1024 or unidade == unidades[-1]:
            return f"{valor:.0f} {unidade}" if unidade == "B" else f"{valor:.2f} {unidade}"
        valor /= 1024
    return f"{n} B"


def formatar_data(mtime: int | float | None) -> str:
    if not mtime:
        return ""
    return datetime.fromtimestamp(mtime, tz=timezone.utc).astimezone().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def juntar(base: str, nome: str) -> str:
    """Junta caminhos sempre com '/', independente do SO local."""
    return nome if base == "/" and nome.startswith("/") else f"{base.rstrip('/')}/{nome}"


def conectar(
    host: str,
    port: int,
    user: str,
    password: str | None,
    key_path: str | None,
    timeout: float,
) -> tuple[paramiko.SFTPClient, paramiko.Transport]:
    transport = paramiko.Transport((host, port))
    transport.banner_timeout = timeout
    try:
        if key_path:
            chave = carregar_chave(os.path.expanduser(key_path))
            transport.connect(username=user, pkey=chave)
        else:
            transport.connect(username=user, password=password)
    except Exception:
        transport.close()
        raise

    sftp = paramiko.SFTPClient.from_transport(transport)
    if sftp is None:  # pragma: no cover - só ocorre se o canal falhar
        transport.close()
        raise RuntimeError("Não foi possível abrir o canal SFTP.")
    sftp.get_channel().settimeout(timeout)
    return sftp, transport


def carregar_chave(caminho: str) -> paramiko.PKey:
    """Tenta os formatos de chave suportados pelo Paramiko, em ordem."""
    senha_chave = os.environ.get("SFTP_KEY_PASSPHRASE")
    erros: list[str] = []
    # DSSKey foi removida no Paramiko 5; monta a lista conforme o que existir.
    classes = [
        getattr(paramiko, nome)
        for nome in ("Ed25519Key", "RSAKey", "ECDSAKey", "DSSKey")
        if hasattr(paramiko, nome)
    ]
    for classe in classes:
        try:
            return classe.from_private_key_file(caminho, password=senha_chave)
        except paramiko.PasswordRequiredException:
            raise SystemExit(
                "Chave privada protegida por senha. Defina SFTP_KEY_PASSPHRASE."
            )
        except Exception as exc:  # formato incompatível: tenta o próximo
            erros.append(f"{classe.__name__}: {exc}")
    raise SystemExit("Não foi possível ler a chave privada:\n  " + "\n  ".join(erros))


def resolver_pasta(sftp: paramiko.SFTPClient, caminho: str) -> str | None:
    """Confirma que a pasta existe; tenta casar o nome ignorando maiuscula/minuscula."""
    try:
        if stat.S_ISDIR(sftp.stat(caminho).st_mode or 0):
            return caminho
    except IOError:
        pass

    limpo = caminho.rstrip("/") or "/"
    pai = os.path.dirname(limpo) or "/"
    alvo = os.path.basename(limpo).lower()
    try:
        for attr in sftp.listdir_attr(pai):
            if attr.filename.lower() == alvo and stat.S_ISDIR(attr.st_mode or 0):
                return juntar(pai, attr.filename)
    except IOError as exc:
        print(f"[aviso] nao consegui listar {pai}: {exc}", file=sys.stderr)
    return None


def listar_pastas(sftp: paramiko.SFTPClient, caminho: str) -> list[str]:
    try:
        return sorted(
            a.filename for a in sftp.listdir_attr(caminho) if stat.S_ISDIR(a.st_mode or 0)
        )
    except IOError:
        return []


def caminhar(
    sftp: paramiko.SFTPClient,
    raiz: str,
    seguir_links: bool = False,
    recursivo: bool = True,
) -> Iterator[Entrada]:
    """Percorre a árvore em profundidade, tolerando diretórios sem permissão."""
    visitados: set[str] = set()
    pilha: list[str] = [raiz]

    while pilha:
        atual = pilha.pop()
        try:
            chave = sftp.normalize(atual)
        except IOError as exc:
            print(f"[aviso] ignorando {atual}: {exc}", file=sys.stderr)
            continue
        if chave in visitados:  # protege contra loops de symlink
            continue
        visitados.add(chave)

        try:
            itens = sftp.listdir_attr(atual)
        except IOError as exc:
            print(f"[aviso] sem acesso a {atual}: {exc}", file=sys.stderr)
            continue

        for attr in sorted(itens, key=lambda a: a.filename):
            caminho = juntar(atual, attr.filename)
            modo = attr.st_mode or 0

            if stat.S_ISLNK(modo):
                if not seguir_links:
                    continue
                try:
                    modo = (sftp.stat(caminho).st_mode) or 0
                except IOError as exc:
                    print(f"[aviso] link quebrado {caminho}: {exc}", file=sys.stderr)
                    continue

            if stat.S_ISDIR(modo):
                if recursivo:
                    pilha.append(caminho)
                continue

            if not stat.S_ISREG(modo):
                continue

            tamanho = attr.st_size or 0
            yield Entrada(
                caminho_completo=caminho,
                nome_arquivo=attr.filename,
                extensao=os.path.splitext(attr.filename)[1].lower(),
                tamanho_bytes=tamanho,
                tamanho_legivel=formatar_tamanho(tamanho),
                data_modificacao=formatar_data(attr.st_mtime),
            )


# --------------------------------------------------------------------------- #
# Leitura e detecção de layout
# --------------------------------------------------------------------------- #


def ler_amostra(sftp: paramiko.SFTPClient, caminho: str, limite: int) -> tuple[bytes, bool]:
    """Baixa no máximo `limite` bytes. Retorna (dados, foi_truncado)."""
    with sftp.open(caminho, "rb") as f:
        f.prefetch()
        dados = f.read(limite + 1)
    truncado = len(dados) > limite
    return dados[:limite], truncado


def decodificar(dados: bytes) -> tuple[str, str]:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return dados.decode(enc), enc
        except UnicodeDecodeError:
            continue
    return dados.decode("latin-1", "replace"), "latin-1 (com perdas)"


def tipo_binario(dados: bytes) -> str | None:
    for assinatura, nome in MAGIC_BINARIO:
        if dados.startswith(assinatura):
            return nome
    # heurística: byte nulo em arquivo texto praticamente não acontece
    if b"\x00" in dados[:4096]:
        return "binário (contém bytes nulos)"
    return None


def detectar_quebra(texto: str) -> str:
    if "\r\n" in texto:
        return "CRLF"
    if "\n" in texto:
        return "LF"
    if "\r" in texto:
        return "CR"
    return "sem quebra de linha"


def detectar_delimitador(linhas: list[str]) -> tuple[str | None, str, int, float]:
    """Retorna (delimitador, nome, n_colunas, consistência 0..1)."""
    melhor: tuple[str | None, str, int, float] = (None, "", 0, 0.0)
    for simbolo, nome in DELIMITADORES:
        contagens = Counter(linha.count(simbolo) for linha in linhas)
        valor, ocorrencias = contagens.most_common(1)[0]
        if valor == 0:
            continue
        consistencia = ocorrencias / len(linhas)
        if consistencia > melhor[3]:
            melhor = (simbolo, nome, valor + 1, consistencia)
    return melhor


def detectar_largura_fixa(linhas: list[str]) -> tuple[int, float]:
    """Retorna (largura predominante, fração das linhas com essa largura)."""
    larguras = Counter(len(linha) for linha in linhas)
    largura, ocorrencias = larguras.most_common(1)[0]
    return largura, ocorrencias / len(linhas)


def visivel(texto: str, largura: int = COLUNAS_EXIBIDAS) -> str:
    limpo = "".join(ch if ch.isprintable() else "." for ch in texto)
    return limpo if len(limpo) <= largura else limpo[:largura] + " [...]"


def regua(largura: int) -> str:
    """Régua de posições, para ler layouts de largura fixa."""
    largura = min(largura, COLUNAS_EXIBIDAS)
    dezenas = "".join(str((i // 10) % 10) if i % 10 == 0 else " " for i in range(largura))
    unidades = "".join(str(i % 10) for i in range(largura))
    return f"{dezenas}\n     {unidades}"


def analisar(nome: str, dados: bytes, truncado: bool, tamanho_total: int) -> None:
    """Imprime um diagnóstico do layout do arquivo."""
    print("=" * 78)
    print(f"ARQUIVO: {nome}")
    print(
        f"Tamanho: {formatar_tamanho(tamanho_total)} ({tamanho_total} bytes) | "
        f"amostra baixada: {len(dados)} bytes"
        + (" (truncada)" if truncado else " (arquivo inteiro)")
    )

    if not dados:
        print("Arquivo vazio.")
        return

    binario = tipo_binario(dados)
    if binario:
        print(f"Formato: {binario} - não é texto plano.")
        print(f"Primeiros bytes: {dados[:32].hex(' ')}")
        return

    texto, encoding = decodificar(dados)
    quebra = detectar_quebra(texto)
    linhas = texto.splitlines()
    if truncado and len(linhas) > 1:
        linhas = linhas[:-1]  # a última pode ter vindo cortada pelo limite
    if not linhas:
        print("Sem linhas legíveis na amostra.")
        return

    print(f"Encoding: {encoding} | quebra de linha: {quebra}")
    print(f"Linhas na amostra: {len(linhas)}")

    largura, prop_largura = detectar_largura_fixa(linhas)
    simbolo, nome_delim, colunas, consistencia = detectar_delimitador(linhas)
    fixo = prop_largura >= 0.9 and largura > 0 and len(set(map(len, linhas))) <= 2

    if fixo and (simbolo is None or consistencia < 0.9 or largura in (240, 400, 150)):
        print(
            f"Layout: LARGURA FIXA, {largura} caracteres por linha "
            f"({prop_largura:.0%} das linhas)."
        )
        if largura == 240:
            print("  Compatível com CNAB 240 (padrão FEBRABAN).")
            print("  Posições: 1-3 banco | 4-7 lote | 8 tipo de registro")
            tipos = Counter(linha[7:8] for linha in linhas if len(linha) >= 8)
            print(f"  Tipos de registro na amostra: {dict(sorted(tipos.items()))}")
        elif largura == 400:
            print("  Compatível com CNAB 400. Posição 1 = tipo de registro.")
            tipos = Counter(linha[0:1] for linha in linhas)
            print(f"  Tipos de registro na amostra: {dict(sorted(tipos.items()))}")
    elif simbolo is not None and consistencia >= 0.7:
        rotulo = "\\t" if simbolo == "\t" else simbolo
        print(
            f"Layout: DELIMITADO por '{rotulo}' ({nome_delim}), {colunas} colunas "
            f"({consistencia:.0%} das linhas com a mesma contagem)."
        )
        cabecalho = [c.strip() for c in linhas[0].split(simbolo)]
        numerico = sum(1 for c in cabecalho if c.replace(".", "").replace(",", "").isdigit())
        if numerico <= len(cabecalho) // 4:
            print("  A 1a linha parece ser CABEÇALHO:")
            for i, coluna in enumerate(cabecalho, 1):
                print(f"    {i:>3}. {coluna}")
    else:
        print("Layout: texto sem delimitador nem largura fixa evidentes.")
        print(f"  Larguras mais comuns: {Counter(len(l) for l in linhas).most_common(3)}")

    print(f"\nPrimeiras {min(LINHAS_EXIBIDAS, len(linhas))} linhas:")
    if fixo:
        print("     " + regua(largura))
    for i, linha in enumerate(linhas[:LINHAS_EXIBIDAS], 1):
        print(f"{i:>3}| {visivel(linha)}")
    print()


def inspecionar(
    sftp: paramiko.SFTPClient,
    caminhos: list[tuple[str, int]],
    limite_bytes: int,
    destino: str | None = None,
) -> dict[str, bytes]:
    """Analisa cada arquivo e devolve os bytes lidos, por caminho."""
    if destino:
        os.makedirs(destino, exist_ok=True)
    lidos: dict[str, bytes] = {}
    for caminho, tamanho in caminhos:
        try:
            dados, truncado = ler_amostra(sftp, caminho, limite_bytes)
        except IOError as exc:
            print(f"[aviso] não foi possível ler {caminho}: {exc}", file=sys.stderr)
            continue
        lidos[caminho] = dados
        analisar(caminho, dados, truncado, tamanho)
        if destino:
            local = os.path.join(destino, os.path.basename(caminho))
            with open(local, "wb") as f:
                f.write(dados)
            print(f"  [salvo] {local}")
            print()
    return lidos


# --------------------------------------------------------------------------- #
# Saída
# --------------------------------------------------------------------------- #


def montar_df(lidos: dict[str, bytes], saida: str | None) -> None:
    """Monta o DataFrame de lançamentos CNAB 240 a partir dos bytes lidos."""
    try:
        import cnab240_extrato as cnab
        import pandas as pd
    except ImportError as exc:
        print(f"[erro] --df exige pandas e cnab240_extrato.py: {exc}", file=sys.stderr)
        return

    registros: list[dict] = []
    for caminho, dados in lidos.items():
        registros.extend(cnab.lancamentos_de_bytes(os.path.basename(caminho), dados))

    if not registros:
        print(
            "\nNenhum lançamento (registro 3 segmento E) nos arquivos amostrados.\n"
            "Extratos de 4 registros (968 B) não têm movimento: use --maiores."
        )
        return

    df = cnab.arrumar(registros)
    print(f"\nDataFrame: {len(df)} lançamento(s) x {len(df.columns)} colunas\n")
    with pd.option_context("display.width", 220, "display.max_columns", 40):
        print(df.to_string(index=False))
    if saida:
        df.to_csv(saida, sep=";", index=False, encoding="utf-8-sig")
        print(f"\nDataFrame gravado em: {os.path.abspath(saida)}")


def gravar_csv(destino: str, entradas: list[Entrada]) -> None:
    campos = list(Entrada.__dataclass_fields__)
    pasta = os.path.dirname(os.path.abspath(destino))
    os.makedirs(pasta, exist_ok=True)
    with open(destino, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=campos, delimiter=";")
        writer.writeheader()
        for e in entradas:
            writer.writerow(asdict(e))


def exibir(entradas: list[Entrada], limite: int) -> None:
    if not entradas:
        print("Nenhum arquivo encontrado com os filtros informados.")
        return

    amostra = entradas[:limite]
    larg_caminho = min(max((len(e.caminho_completo) for e in amostra), default=20), 80)
    larg_nome = min(max((len(e.nome_arquivo) for e in amostra), default=20), 40)

    cabecalho = (
        f"{'CAMINHO COMPLETO':<{larg_caminho}}  {'ARQUIVO':<{larg_nome}}  "
        f"{'TAMANHO':>12}  {'MODIFICADO EM':<19}"
    )
    print(cabecalho)
    print("-" * len(cabecalho))
    for e in amostra:
        caminho = truncar(e.caminho_completo, larg_caminho)
        nome = truncar(e.nome_arquivo, larg_nome)
        print(
            f"{caminho:<{larg_caminho}}  {nome:<{larg_nome}}  "
            f"{e.tamanho_legivel:>12}  {e.data_modificacao:<19}"
        )

    if len(entradas) > limite:
        print(f"\n... e mais {len(entradas) - limite} registro(s) apenas no CSV.")


def truncar(texto: str, largura: int) -> str:
    return texto if len(texto) <= largura else "..." + texto[-(largura - 3):]


def normalizar_ext(valores: list[str]) -> set[str]:
    return {v if v.startswith(".") else f".{v}" for v in (x.lower().strip() for x in valores)}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Lista uma pasta do SFTP e interpreta o formato dos arquivos.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Credenciais: SFTP_USER / SFTP_PASSWORD no .env ao lado do script.",
    )
    p.add_argument("--host", default=os.environ.get("SFTP_HOST", DEFAULT_HOST))
    p.add_argument("--port", type=int, default=int(os.environ.get("SFTP_PORT", DEFAULT_PORT)))
    p.add_argument(
        "--user",
        default=os.environ.get("SFTP_USER"),
        help="Usuário SFTP (ou variável SFTP_USER / pwd.env).",
    )
    p.add_argument(
        "--path",
        default=PASTA_PADRAO,
        help=f"Pasta a listar (padrão: {PASTA_PADRAO}; ou variável SFTP_PATH).",
    )
    p.add_argument(
        "--ext",
        nargs="+",
        metavar="EXT",
        help=f"Filtra por extensão, ex.: {' '.join(EXTENSOES_SUGERIDAS)}",
    )
    p.add_argument(
        "--out",
        default="inventario_sftp.csv",
        help="Arquivo CSV de saída (padrão: inventario_sftp.csv).",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=PREVIEW_LIMIT,
        help=f"Quantidade de registros exibidos no terminal (padrão: {PREVIEW_LIMIT}).",
    )
    p.add_argument("--key", help="Caminho de uma chave privada, no lugar da senha.")
    p.add_argument(
        "--follow-symlinks",
        action="store_true",
        help="Segue links simbólicos (desligado por padrão).",
    )
    p.add_argument("--timeout", type=float, default=30.0, help="Timeout em segundos.")
    p.add_argument(
        "--recursivo",
        action="store_true",
        help="Desce também nas subpastas (por padrão lista só a pasta informada).",
    )

    g = p.add_argument_group("inspeção de conteúdo")
    g.add_argument(
        "--arquivo",
        nargs="+",
        metavar="CAMINHO",
        help="Analisa estes arquivos e encerra (não lista a pasta). Aceita vários.",
    )
    g.add_argument(
        "--amostra",
        type=int,
        default=1,
        metavar="N",
        help="Analisa o formato dos N primeiros arquivos listados (0 desliga; padrão: 1).",
    )
    g.add_argument(
        "--amostra-bytes",
        type=int,
        default=AMOSTRA_BYTES,
        help=f"Bytes baixados de cada arquivo analisado (padrão: {AMOSTRA_BYTES}).",
    )
    g.add_argument(
        "--baixar",
        metavar="PASTA",
        help="Salva os arquivos analisados nesta pasta local (para inspeção offline).",
    )
    g.add_argument(
        "--maiores",
        action="store_true",
        help="Amostra os arquivos MAIORES em vez dos primeiros em ordem alfabética.",
    )
    g.add_argument(
        "--df",
        action="store_true",
        help="Monta o DataFrame de lançamentos CNAB 240 dos arquivos amostrados.",
    )
    g.add_argument("--df-saida", metavar="CSV", help="Grava o DataFrame neste CSV.")
    g.add_argument("--sem-csv", action="store_true", help="Não gera o CSV do inventário.")
    return p.parse_args(argv)


def perguntar(texto: str, secreto: bool = False) -> str:
    """Pergunta no terminal; devolve "" se nao houver entrada interativa."""
    try:
        return (getpass(texto) if secreto else input(texto)).strip()
    except (EOFError, KeyboardInterrupt):
        print(file=sys.stderr)
        return ""


def main(argv: list[str] | None = None) -> int:
    carregar_env(ENV_FILES)
    args = parse_args(argv)

    if not args.user:
        args.user = perguntar("Usuário SFTP: ")
    if not args.user:
        print("Erro: usuário não informado (use --user ou SFTP_USER).", file=sys.stderr)
        return 2

    senha = None
    if not args.key:
        senha = os.environ.get("SFTP_PASSWORD") or perguntar(
            "Senha SFTP (não será exibida): ", secreto=True
        )
        if not senha:
            print(
                "Erro: defina SFTP_PASSWORD (variável de ambiente ou pwd.env) "
                "ou use --key para autenticação por chave.",
                file=sys.stderr,
            )
            return 2

    filtro = normalizar_ext(args.ext) if args.ext else None

    print(f"Conectando em {args.user}@{args.host}:{args.port} ...", file=sys.stderr)
    try:
        sftp, transport = conectar(
            args.host, args.port, args.user, senha, args.key, args.timeout
        )
    except paramiko.AuthenticationException:
        print("Erro: falha de autenticação (usuário, senha ou chave inválidos).", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Erro ao conectar: {exc}", file=sys.stderr)
        return 1

    # Modo 1: analisar um único arquivo indicado na linha de comando.
    if args.arquivo:
        try:
            alvos = []
            for caminho in args.arquivo:
                try:
                    alvos.append((caminho, sftp.stat(caminho).st_size or 0))
                except IOError as exc:
                    print(f"[aviso] nao consegui abrir {caminho}: {exc}", file=sys.stderr)
            if not alvos:
                return 1
            limite = max(t for _, t in alvos) if args.df else args.amostra_bytes
            lidos = inspecionar(sftp, alvos, limite, args.baixar)
            if args.df:
                montar_df(lidos, args.df_saida)
        finally:
            sftp.close()
            transport.close()
        return 0

    # Modo 2: listagem da pasta (recursiva apenas com --recursivo).
    pasta = resolver_pasta(sftp, args.path)
    if pasta is None:
        print(f"Erro: pasta {args.path} nao encontrada no servidor.", file=sys.stderr)
        disponiveis = listar_pastas(sftp, "/")
        if disponiveis:
            print("Pastas na raiz: " + ", ".join(disponiveis), file=sys.stderr)
        sftp.close()
        transport.close()
        return 1
    if pasta != args.path:
        print(f"[info] usando a pasta {pasta}", file=sys.stderr)
    args.path = pasta

    entradas: list[Entrada] = []
    try:
        for entrada in caminhar(
            sftp,
            args.path,
            seguir_links=args.follow_symlinks,
            recursivo=args.recursivo,
        ):
            if filtro and entrada.extensao not in filtro:
                continue
            entradas.append(entrada)
            if len(entradas) % 500 == 0:
                print(f"  ... {len(entradas)} arquivos", file=sys.stderr)
    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário; gravando o que já foi coletado.", file=sys.stderr)

    entradas.sort(key=lambda e: e.caminho_completo)

    try:
        if args.amostra > 0 and entradas:
            escolhidos = (
                sorted(entradas, key=lambda e: e.tamanho_bytes, reverse=True)
                if args.maiores
                else entradas
            )
            alvos = [(e.caminho_completo, e.tamanho_bytes) for e in escolhidos[: args.amostra]]
            # com --df o arquivo tem de vir inteiro, senao o ultimo registro fica cortado
            limite = max(t for _, t in alvos) if args.df else args.amostra_bytes
            print(f"\nAnalisando o formato de {len(alvos)} arquivo(s):\n")
            lidos = inspecionar(sftp, alvos, limite, args.baixar)
            if args.df:
                montar_df(lidos, args.df_saida)
    finally:
        sftp.close()
        transport.close()

    if not args.sem_csv:
        gravar_csv(args.out, entradas)

    exibir(entradas, args.limit)
    total_bytes = sum(e.tamanho_bytes for e in entradas)
    print(f"\nTotal: {len(entradas)} arquivo(s), {formatar_tamanho(total_bytes)}.")
    if not args.sem_csv:
        print(f"CSV gerado em: {os.path.abspath(args.out)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
