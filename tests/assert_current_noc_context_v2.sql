select *
from {{ ref('fct_viabilidade_vagas') }}
where codigo_profissao is not null
  and (
      metodo_classificacao_profissao <> 'gemini_context_fingerprint'
      or versao_prompt_profissao <> 'noc_context_v2'
      or chave_classificacao_profissao is null
      or evidencia_profissao is null
  )
