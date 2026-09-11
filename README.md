# Extratos CNAB 240 do SFTP → um dataset só

Lê os extratos bancários (`.RET`, CNAB 240) que os bancos depositam na pasta
`/EXTRATO` do SFTP e mantém **um arquivo final único** com um lançamento por
linha — atualizado de forma incremental, sem baixar de novo o que já foi baixado
e sem duplicar lançamento que já está no dataset.

O lago já tem arquivos dos bancos **341** (Itaú), **033** (Santander) e **655** —
os três usam as mesmas posições no bloco de movimentação, e os saldos de todos os
lotes fecham. `medalhao.py status` mostra os números do momento.

---

## Instalação

```bash
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
```

Testado com Python 3.14, pandas 3.0 e paramiko 5.0. `pyarrow` é opcional, só
para gravar prata/ouro em parquet.

### Credenciais

Crie um `.env` ao lado dos scripts. Ele está no `.gitignore` — **nunca**
versione, e nunca coloque senha no código:

```
SFTP_USER=usuario@dominio
SFTP_PASSWORD=a-senha
```

Opcionais: `SFTP_HOST` (padrão `sftp-drive.opea.solutions`), `SFTP_PORT` (22),
`SFTP_PATH` (`/EXTRATO`), `LAKE_PATH` (`lake`), `SFTP_ENV_FILE` para apontar
outro arquivo de credenciais. Em vez de senha dá para usar `--key CAMINHO` com
uma chave privada (`SFTP_KEY_PASSPHRASE` se ela tiver senha).

---

## O dia a dia

```bash
.venv/Scripts/python.exe medalhao.py sftp
```

É isso. Lista o `/EXTRATO`, baixa **só** os arquivos que ainda não foram
ingeridos, extrai os lançamentos e reescreve o arquivo final em
`lake/ouro/extrato_lancamentos.csv`. Rodar duas vezes seguidas não muda nada.

| Comando | O que faz |
|---|---|
| `medalhao.py sftp` | a carga: baixa o que falta e atualiza o arquivo final |
| `medalhao.py status` | painel das três camadas: quantos, período, se os saldos fecham |
| `medalhao.py local "amostras/*.RET"` | ingere arquivos que já estão no disco |
| `medalhao.py reprocessar` | refaz prata e ouro a partir do bronze, sem tocar no SFTP |
| `medalhao.py --help` | todas as opções |

Rodar o arquivo **sem argumento** (o botão Run da IDE) mostra o `status`, que só
lê o lago: não conecta em nada e não grava nada.

Opções úteis do `sftp`: `--limite N` (ingere no máximo N arquivos novos nesta
rodada — bom para o primeiro teste), `--path`, `--recursivo`, `--ext`,
`--forcar` (reingere mesmo o que já está no manifesto), `--lake PASTA`,
`--formato parquet`.

> A carga inicial já foi feita: o `/EXTRATO` tem cerca de 1.900 arquivos e todos
> estão no bronze. Ela é sequencial, um arquivo por vez, então uma carga do zero
> leva alguns minutos. Se interromper com `Ctrl-C`, o que já foi gravado no
> bronze fica no manifesto e a rodada seguinte continua de onde parou.
>
> A maioria dos arquivos (cerca de 1.350) é extrato de 4 registros, **sem
> movimento** — entra no manifesto como `sem_movimento` e não gera linha no
> dataset. Isso é o esperado, não erro.

---

## Os três módulos

| Arquivo | Papel |
|---|---|
| [`medalhao.py`](medalhao.py) | o pipeline: decide o que ingerir, dedupliza e monta o dataset |
| [`sftp_inventario.py`](sftp_inventario.py) | conexão, listagem e diagnóstico de layout de arquivo desconhecido |
| [`cnab240_extrato.py`](cnab240_extrato.py) | tudo que é específico do formato CNAB 240: posições, lotes, saldos |

A dependência é numa direção só: `medalhao.py` usa os outros dois; nenhum dos
dois conhece o pipeline.

---

## As camadas do lago

```
lake/
├── bronze/                      # a fonte da verdade
│   ├── _manifesto.csv           # o que já entrou: nome, tamanho, mtime, sha256
│   ├── 341/2026-08/*.RET        # o arquivo cru, byte a byte, como veio do banco
│   ├── 341/2026-09/*.RET        # particionado por banco e competência
│   └── 033/2026-09/*.RET
├── prata/
│   ├── lancamentos.csv          # 1 lançamento/linha + linhagem, sem duplicatas
│   └── conferencia.csv
└── ouro/
    ├── extrato_lancamentos.csv  # ← O ARQUIVO FINAL
    └── conferencia_saldos.csv
```

**Bronze** guarda o `.RET` intacto porque o layout do bloco de movimentação
(posições 103–240) foi *medido* nos arquivos reais, não lido de uma
especificação. Quando um banco mudar uma posição, você corrige
`cnab240_extrato.py` e roda `reprocessar` — prata e ouro são refeitos do zero,
sem depender de o banco ainda ter o arquivo no servidor.

**Prata** tem as 45 colunas do dataset mais três de linhagem
(`id_lancamento`, `hash_arquivo`, `ingerido_em`), que respondem "de qual arquivo
veio esta linha e quando".

**Ouro** tira as três de controle, ordena por banco/agência/conta/data/lote e
grava exatamente as 45 colunas do DataFrame do `cnab240_extrato`. É o arquivo que
se entrega. CSV com `;`, `utf-8-sig`, datas em `AAAA-MM-DD`.

O `lake/` está no `.gitignore`: é dado, não código.

---

## O arquivo final: as 45 colunas

**Identificação** (vem do nome do arquivo, `ext_<banco>_<agência>_<conta>_<data>`)
`arquivo`, `banco`, `agencia`, `agencia_id`, `conta`, `conta_5dig`,
`conta_sem_dv`, `data_arquivo`, `sequencia`

**Lote e saldos** (dos registros 1 e 5)
`n_lote`, `moeda`, `data_saldo_inicial`, `saldo_inicial`, `data_saldo_final`,
`saldo_final`, `total_debitos_lote`, `total_creditos_lote`, `qtd_registros_lote`

**O lançamento** (registro 3, segmento E)
`lote`, `sequencial`, `segmento`, `data_contabil`, `data_lancamento`,
`tipo_complemento`, `complemento`, `codigo_historico`, `historico`, `documento`,
`situacao`, `debito_credito`, `valor_centavos`, `valor`, `valor_assinado`

**Conferência do registro** (para checar contra o nome do arquivo)
`nome_empresa`, `tipo_inscricao`, `inscricao`, `convenio`, `agencia_reg`,
`agencia_dv`, `conta_reg`, `conta_dv`, `dv_agencia_conta`, `banco_reg`,
`registro`, `qtd_registros_arquivo`

Para somar movimento use **`valor_assinado`** (negativo em débito). `valor` é
sempre positivo, e `valor_centavos` é o inteiro cru do registro, sem risco de
arredondamento.

`agencia_id` é a agência sem zeros à esquerda (`0350` → `350`) e `conta_5dig` são
os últimos 5 dígitos — as duas chaves de negócio usadas para casar com os
sistemas internos. Ao ler o CSV em pandas, **force `dtype=str`** nas colunas de
agência e conta, senão `"0350"` vira o inteiro `350`:

```python
ident = ["banco", "agencia", "agencia_id", "conta", "conta_5dig", "conta_sem_dv"]
df = pd.read_csv("lake/ouro/extrato_lancamentos.csv", sep=";",
                 encoding="utf-8-sig", dtype={c: str for c in ident})
```

---

## Como a carga incremental funciona

Dois níveis, resolvendo problemas diferentes:

**1. Arquivo** — evita tráfego. A chave é `(nome, tamanho, data de modificação)`,
que sai da própria listagem do SFTP: se não mudou, o arquivo **não é baixado**. O
`sha256` cobre o mesmo conteúdo reenviado com outro nome — entra no manifesto
como `duplicado` e não é reprocessado. Se o **mesmo nome** vier com conteúdo
diferente (extrato parcial pela manhã, completo à noite), as duas versões
convivem no bronze, a segunda com sufixo `__<sha>`.

**2. Registro** — evita duplicata no dataset. Cada lançamento recebe um
`id_lancamento` = sha1 da chave de negócio: banco + agência + conta + data do
lançamento + valor + D/C + código do histórico + histórico + documento +
complemento, mais um índice de **ocorrência** que distingue dois lançamentos
legitimamente idênticos no mesmo dia. O nome do arquivo e o sequencial do
registro **não** entram na chave — é isso que faz o mesmo lançamento, reenviado
noutro arquivo, gerar o mesmo id e ser reconhecido.

Na prática: um extrato parcial com 20 lançamentos, depois o completo do mesmo dia
com 78, resulta em 78 linhas — não 98.

---

## O script não apaga nada do SFTP

As únicas operações feitas no servidor são `listdir_attr`, `stat`, `normalize` e
`open(caminho, "rb")` — todas de leitura. Não existe no código nenhuma chamada a
`remove`, `unlink`, `rename`, `put`, `mkdir` ou equivalente. O bronze é uma cópia
byte a byte; o original permanece no `/EXTRATO`.

Isso é uma propriedade do código, não uma garantia do servidor: se quiser a
garantia do outro lado, peça à infra uma credencial somente-leitura para a pasta.

---

## Quando algo dá errado

**Nada acontece ao rodar.** Sem subcomando o script mostra o `status`. Se ele
mostrou o painel, ele rodou — a carga é `medalhao.py sftp`.

**`ModuleNotFoundError`.** Está usando o Python errado. Use
`.venv/Scripts/python.exe medalhao.py ...` ou selecione o interpretador do
`.venv` na IDE.

**A conferência não fecha 100%.** O `status` mostra
`conferencia: N/M lote(s) fecham`. Cada lote tem de satisfazer
`saldo inicial + soma dos lançamentos = saldo final`, aritmética do próprio
banco — se não fecha, o parser divergiu do arquivo. Para achar onde:

```bash
.venv/Scripts/python.exe cnab240_extrato.py "lake/bronze/**/*.RET" --conferir
.venv/Scripts/python.exe cnab240_extrato.py lake/bronze/341/2026-09/ARQUIVO.RET --posicoes
```

O `--posicoes` imprime o mapa de cada registro com régua de colunas, as datas e
os valores candidatos encontrados, e o mapeamento atual campo por campo. Ajuste
`MOVIMENTACAO` em `cnab240_extrato.py` e rode `medalhao.py reprocessar`.

**`[conferir] ... nome do arquivo diz X, posição N do registro tem Y`.** A
identificação de agência/conta sai do nome do arquivo e é checada contra o
conteúdo. Esse aviso diz que as duas não casam — e mostra em que posição o valor
realmente aparece.

**`[aviso] nome fora do padrao ext_banco_agencia_conta_data`.** Banco novo com
outra convenção de nome. Os campos de identificação vêm vazios (o
`id_lancamento` passa a usar o nome do arquivo, para não colidir). Acrescente o
padrão em `RE_NOME`.

**Arquivo em formato desconhecido.** A bancada de inspeção deduz encoding,
quebra de linha, delimitador ou largura fixa, e reconhece CNAB 240/400:

```bash
.venv/Scripts/python.exe sftp_inventario.py --arquivo /EXTRATO/NOVO.RET
.venv/Scripts/python.exe sftp_inventario.py --path /EXTRATO --amostra 5 --maiores
```

Sem `--sem-csv` ela também grava o inventário completo da pasta em
`inventario_sftp.csv` (nome, tamanho, data de modificação de cada arquivo).

---

## Limites conhecidos

- **A identificação da conta vem do nome do arquivo**, não do registro. É o que o
  `conferir_layout()` valida a cada ingestão, mas um banco novo com outra
  convenção de nome precisa de um padrão novo em `RE_NOME`.
- **Deleção não é propagada.** Arquivo apagado do `/EXTRATO` continua no bronze,
  de propósito — o bronze é histórico. A consequência é que se um banco corrigir
  um lançamento reenviando o dia *sem* ele, o lançamento antigo permanece no
  dataset. Se isso aparecer na prática, o caminho é marcar a versão vigente por
  (conta, data) usando `hash_arquivo` e `ingerido_em`, que já estão na prata.
- **Cada rodada reescreve prata e ouro por inteiro**, em vez de fazer append. No
  volume atual é irrelevante e compra determinismo: o mesmo bronze sempre produz
  o mesmo ouro, e uma interrupção não deixa CSV cortado no meio de uma linha.
- **A carga é sequencial**, um arquivo por vez.
