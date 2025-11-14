import re

def remover_caracteres_de_controle(texto):
    """
    Remove quebras de linha, tabs e normaliza espaços, preservando o conteúdo significativo.
    """
    if not isinstance(texto, str):
        # Trata entradas não-string, retornando vazio ou lançando um erro, conforme o requisito.
        return ""

    # 1. Substitui TODAS as quebras de linha (\r, \n), tabs (\t) e outros
    # caracteres de espaço em branco (exceto o espaço regular ' ') por um espaço.
    # O '\s' inclui [\t\n\r\f\v], mas usaremos uma abordagem mais focada
    # para garantir que apenas os 'extras' sejam substituídos por um espaço.

    # A expressão REGEX a seguir substitui QUALQUER tipo de caractere de quebra de linha
    # ou tabulação por um ESPAÇO.
    texto_limpo = re.sub(r"[\r\n\t]+", " ", texto + "!!!!!!!!!!!!!")

    # 2. Remove múltiplos espaços consecutivos (normalizando para um único espaço)
    texto_limpo = re.sub(r"\s{2,}", " ", texto_limpo)

    # 3. Remove espaços extras no início e no fim
    return texto_limpo.strip()

def tratar_referencia(entrada):
    """
    Trata o array de strings fornecido como entrada, aplicando a limpeza de caracteres
    de controle em cada elemento e devolvendo o mesmo array limpo.
    """
    # Verifica se a entrada é uma lista
    if isinstance(entrada, list):
        # Processa cada elemento do array
        return [remover_caracteres_de_controle(item) for item in entrada]

    # Se a entrada não for uma lista, retorna uma lista vazia
    # Se a entrada fosse uma string, poderia-se retornar [remover_caracteres_de_controle(entrada)]
    # mas mantendo o requisito de retornar uma lista:
    return []

# Exemplo de uso:
# dados = [" Linha 1\r\n", " Linha 2 \t com tab ", "Linha 3\n\nFinal. "]
# resultado = tratar_referencia(dados)
# print(resultado) # Deve retornar: ['Linha 1', 'Linha 2 com tab', 'Linha 3 Final.



if __name__ == "__main__":
    x = [ "LEG:FED SUM:****** ANO:****\n*****  SUM(STJ)    SÚMULA DO SUPERIOR TRIBUNAL DE JUSTIÇA\n        SUM:000340", "LEG:FED LEI:005890 ANO:1973", "LEG:FED DEC:089312 ANO:1984\n*****  CLPS-84    CONSOLIDAÇÃO DAS LEIS DA PREVIDÊNCIA SOCIAL\n        ART:00021 ART:00023", "LEG:FED LEI:008213 ANO:1991\n*****  LBPS-91    LEI DE BENEFÍCIOS DA PREVIDÊNCIA SOCIAL\n        ART:00041 ART:00103", "LEG:FED LEI:003807 ANO:1960\n*****  LOPS-60    LEI ORGÂNICA DA PREVIDÊNCIA SOCIAL\n        ART:00023\n(ALTERADA PELA LEI 5.890/1973)", "LEG:FED SUM:****** ANO:****\n*****  SUM(STF)    SÚMULA DO SUPREMO TRIBUNAL FEDERAL\n        SUM:000359", "LEG:FED LEI:013105 ANO:2015\n*****  CPC-15    CÓDIGO DE PROCESSO CIVIL DE 2015\n        ART:01036", "LEG:FED EMC:000020 ANO:1998\n        ART:00014", "LEG:FED CFB:****** ANO:1988\n*****  ADCT-88    ATO DAS DISPOSIÇÕES CONSTITUCIONAIS TRANSITÓRIAS\n        ART:00058", "LEG:FED EMC:000041 ANO:2003\n        ART:00005", "LEG:FED DEC:077077 ANO:1976\n*****  CLPS-76    CONSOLIDAÇÃO DAS LEIS DA PREVIDÊNCIA SOCIAL\n        ART:00026 ART:00028", "LEG:FED DEC:083080 ANO:1979\n*****  RBPS-79    REGULAMENTO DOS BENEFÍCIOS DA PREVIDENCIA SOCIAL\n        ART:00036 INC:00002 LET:C ART:00040 ART:00041\n        INC:00004" ]
    print(tratar_referencia(x));
    print("-----")
    x = [ "AgInt na ExeMS  18409 DF 2018/0229287-0 Decisão:02/09/2024\nDJE        DATA:05/09/2024", "AgInt na ExeMS  20656 DF 2017/0310965-1 Decisão:02/09/2024\nDJE        DATA:05/09/2024", "AgInt na ExeMS  22074 DF 2019/0143807-9 Decisão:02/09/2024\nDJE        DATA:05/09/2024", "AgInt na ExeMS  13881 DF 2019/0039651-8 Decisão:20/08/2024\nDJE        DATA:23/08/2024", "AgInt na ExeMS  15372 DF 2018/0007822-6 Decisão:20/08/2024\nDJE        DATA:23/08/2024", "AgInt na ExeMS  15657 DF 2019/0060639-4 Decisão:20/08/2024\nDJE        DATA:23/08/2024", "AgInt na ExeMS  20418 DF 2019/0040469-8 Decisão:20/08/2024\nDJE        DATA:23/08/2024", "AgInt na ExeMS  23009 DF 2019/0069040-5 Decisão:20/08/2024\nDJE        DATA:23/08/2024", "AgInt na ExeMS  23025 DF 2019/0135765-0 Decisão:20/08/2024\nDJE        DATA:23/08/2024", "AgInt na ExeMS  23138 DF 2018/0165234-0 Decisão:20/08/2024\nDJE        DATA:23/08/2024", "AgInt na ImpExe na ExeMS  15570 DF 2019/0067537-3 Decisão:20/08/2024\nDJE        DATA:23/08/2024" ]
    print(tratar_referencia(x));


