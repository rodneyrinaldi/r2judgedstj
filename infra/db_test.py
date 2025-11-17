from db_insert import inserir_dados

# Dados fictícios para teste
dados_teste = {
    "numeroProcesso": "123456789",
    "numeroRegistro": "987654321",
    "siglaClasse": "AC",
    "descricaoClasse": "Ação Civil",
    "nomeOrgaoJulgador": "Tribunal Superior",
    "ministroRelator": "João Silva",
    "ementa": "Ementa do processo fictício",
    "tipoDeDecisao": "Monocrática",
    "dataDecisao": "2025-10-22",
    "decisao": "Decisão fictícia do processo",
    "jurisprudenciaCitada": "Jurisprudência fictícia",
    "notas": "Notas adicionais fictícias",
    "informacoesComplementares": "Informações complementares fictícias",
    "termosAuxiliares": "Termos auxiliares fictícios",
    "teseJuridica": "Tese jurídica fictícia",
    "tema": "Tema fictício",
    "referenciasLegislativas": "Referências legislativas fictícias",
    "acordaosSimilares": "Acórdãos similares fictícios",
    "dataPublicacao": "2025-10-23"
}

# Testa a inserção dos dados
if __name__ == "__main__":
    print("Iniciando teste de inserção...")
    inserir_dados(dados_teste)