import streamlit as st
from pygwalker.api.streamlit import StreamlitRenderer
import pandas as pd
import xml.etree.ElementTree as ET
import requests
import plotly.graph_objects as go
from zeep import Client
from zeep.transports import Transport

# ============================================================
# CONFIGURAÇÕES PADRÃO (fallback)
# ============================================================
WSDL_SUFIXO = "/wsConsultaSQL/MEX?wsdl"
SISTEMA     = "P"
SENTENCA    = "FICHA_FINANCEIRA"
# ============================================================

MESES = {1:"Jan", 2:"Fev", 3:"Mar", 4:"Abr", 5:"Mai", 6:"Jun",
         7:"Jul", 8:"Ago", 9:"Set", 10:"Out", 11:"Nov", 12:"Dez"}

def fmt(valor: float) -> str:
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def buscar_dados(coligada: int, ano: int) -> pd.DataFrame:
    """Conecta ao Web Service do RM e retorna os dados como DataFrame."""
    wsdl_url   = st.session_state.get("wsdl_url")
    rm_usuario = st.session_state.get("rm_usuario")
    rm_senha   = st.session_state.get("rm_senha")

    try:
        session = requests.Session()
        session.auth = (rm_usuario, rm_senha)
        transport = Transport(session=session)

        client = Client(wsdl_url, transport=transport)
        service = client.bind("wsConsultaSQL", "RM_IwsConsultaSQL")

        parameters = f"CODCOLIGADA={coligada};ANO={ano}"

        resultado = service.RealizarConsultaSQL(
            codSentenca=SENTENCA,
            codColigada=0,
            codSistema=SISTEMA,
            parameters=parameters
        )

        root = ET.fromstring(resultado)

        registros = []
        for item in root.findall("Resultado"):
            registros.append({
                "Coligada":    item.findtext("CODCOLIGADA"),
                "Empresa":     item.findtext("NOMEFANTASIA"),
                "Nome":        item.findtext("NOME"),
                "Função":      item.findtext("FUNCAO"),
                "Seção":       item.findtext("SECAO"),
                "Tipo Evento": item.findtext("TIPO_EVENTO"),
                "Evento":      item.findtext("EVENTO"),
                "Período":     item.findtext("NROPERIODO"),
                "Mês":         int(item.findtext("MESCOMP") or 0),
                "Ano":         int(item.findtext("ANOCOMP") or 0),
                "Valor":       float(item.findtext("VALOR") or 0),
                "Liquido":     float(item.findtext("VLR_PROV_DESC") or 0)
            })

        return pd.DataFrame(registros)

    except Exception as e:
        st.error(f"Erro ao buscar dados: {e}")
        return pd.DataFrame()


def grafico_proventos_descontos_saldo(df: pd.DataFrame):
    grp = df.groupby(["Ano", "Mês", "Tipo Evento"])["Valor"].sum().reset_index()
    grp["Período"] = grp["Mês"].astype(str).str.zfill(2) + "/" + grp["Ano"].astype(str)
    pivot = grp.pivot_table(index="Período", columns="Tipo Evento", values="Valor", aggfunc="sum").fillna(0).reset_index()
    pivot = pivot.sort_values("Período")

    provento = pivot.get("Provento", pd.Series([0]*len(pivot)))
    desconto = pivot.get("Desconto", pd.Series([0]*len(pivot)))
    saldo    = provento - desconto

    fig = go.Figure()
    fig.add_trace(go.Bar(x=pivot["Período"], y=provento, name="Proventos", marker_color="#2ecc71",
        text=provento.apply(fmt), textposition="inside"))
    fig.add_trace(go.Bar(x=pivot["Período"], y=desconto, name="Descontos", marker_color="#e74c3c",
        text=desconto.apply(fmt), textposition="inside"))
    fig.add_trace(go.Scatter(x=pivot["Período"], y=saldo, name="Saldo Líquido",
        mode="lines+markers+text", line=dict(color="#f39c12", width=3), marker=dict(size=8),
        text=saldo.apply(fmt), textposition="top center", textfont=dict(color="#f39c12", size=11)))

    fig.update_layout(barmode="stack", title="📊 Proventos x Descontos por Período + Saldo Líquido",
        xaxis_title="Período", yaxis_title="Valor (R$)", height=450,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font=dict(color="white"),
        xaxis=dict(gridcolor="rgba(255,255,255,0.1)"), yaxis=dict(gridcolor="rgba(255,255,255,0.1)"))
    return fig


def grafico_ranking_eventos(df: pd.DataFrame):
    grp = df.groupby(["Evento", "Tipo Evento"])["Valor"].sum().reset_index()
    grp = grp.sort_values("Valor", ascending=True).tail(10)
    colors = grp["Tipo Evento"].map({"Provento": "#2ecc71", "Desconto": "#e74c3c"}).fillna("#95a5a6")

    fig = go.Figure(go.Bar(x=grp["Valor"], y=grp["Evento"], orientation="h",
        marker_color=colors, text=grp["Valor"].apply(fmt), textposition="outside"))
    fig.update_layout(title="🏆 Top 10 Eventos por Valor Total", xaxis_title="Valor Total (R$)",
        yaxis_title="", height=400, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"), xaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.1)"))
    return fig


def grafico_evolucao_saldo(df: pd.DataFrame):
    grp = df.groupby(["Ano", "Mês", "Tipo Evento"])["Valor"].sum().reset_index()
    pivot = grp.pivot_table(index=["Ano", "Mês"], columns="Tipo Evento", values="Valor", aggfunc="sum").fillna(0).reset_index()
    pivot["Período"] = pivot["Mês"].astype(str).str.zfill(2) + "/" + pivot["Ano"].astype(str)
    pivot = pivot.sort_values(["Ano", "Mês"])
    pivot["Saldo"] = pivot.get("Provento", 0) - pivot.get("Desconto", 0)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=pivot["Período"], y=pivot["Saldo"], mode="lines+markers",
        fill="tozeroy", line=dict(color="#f39c12", width=2), marker=dict(size=6),
        fillcolor="rgba(243,156,18,0.2)", name="Saldo Líquido"))
    fig.update_layout(title="📈 Evolução do Saldo Líquido", xaxis_title="Período",
        yaxis_title="Saldo (R$)", height=350, plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)", font=dict(color="white"),
        xaxis=dict(gridcolor="rgba(255,255,255,0.1)"), yaxis=dict(gridcolor="rgba(255,255,255,0.1)"))
    return fig



def grafico_gastos_funcao(df: pd.DataFrame, coluna: str = "Valor"):
    grp = df.groupby("Função")[coluna].sum().reset_index()
    grp = grp.sort_values(coluna, ascending=True).tail(10)
    label = "Valor Líquido (R$)" if coluna == "Liquido" else "Valor Total (R$)"

    fig = go.Figure(go.Bar(x=grp[coluna], y=grp["Função"], orientation="h",
        marker_color="#3498db", text=grp[coluna].apply(fmt), textposition="outside"))
    fig.update_layout(title="👔 Gastos por Função (Top 10)", xaxis_title=label,
        yaxis_title="", height=400, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"), xaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.1)"))
    return fig


def grafico_gastos_secao(df: pd.DataFrame, coluna: str = "Valor"):
    grp = df.groupby("Seção")[coluna].sum().reset_index()
    grp = grp.sort_values(coluna, ascending=True).tail(10)
    label = "Valor Líquido (R$)" if coluna == "Liquido" else "Valor Total (R$)"

    fig = go.Figure(go.Bar(x=grp[coluna], y=grp["Seção"], orientation="h",
        marker_color="#9b59b6", text=grp[coluna].apply(fmt), textposition="outside"))
    fig.update_layout(title="🏢 Gastos por Seção (Top 10)", xaxis_title=label,
        yaxis_title="", height=400, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"), xaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.1)"))
    return fig


def grafico_comprometimento(df: pd.DataFrame, limiar: float, agrupamento: str):
    """Gráfico de barras com índice de comprometimento (Descontos / Proventos) por agrupamento."""
    col = agrupamento  # "Nome", "Seção" ou "Função"

    prov = df[df["Tipo Evento"] == "Provento"].groupby(col)["Valor"].sum().rename("Proventos")
    desc = df[df["Tipo Evento"] == "Desconto"].groupby(col)["Valor"].sum().rename("Descontos")
    grp  = pd.concat([prov, desc], axis=1).fillna(0).reset_index()
    grp  = grp[grp["Proventos"] > 0].copy()
    grp["Índice (%)"] = (grp["Descontos"] / grp["Proventos"] * 100).round(1)
    grp = grp.sort_values("Índice (%)", ascending=True).tail(20)

    # Para agrupamento por Nome, enriquecer com Seção e Função
    if col == "Nome":
        info = df[["Nome", "Seção", "Função"]].drop_duplicates("Nome").set_index("Nome")
        grp["Seção"]  = grp["Nome"].map(info["Seção"]).fillna("-")
        grp["Função"] = grp["Nome"].map(info["Função"]).fillna("-")
        customdata = grp[["Proventos", "Descontos", "Seção", "Função"]].values
        hovertemplate = (
            "<b>%{y}</b><br>"
            "Seção: %{customdata[2]}<br>"
            "Função: %{customdata[3]}<br>"
            "Índice: %{x:.1f}%<br>"
            "Proventos: R$ %{customdata[0]:,.2f}<br>"
            "Descontos: R$ %{customdata[1]:,.2f}<extra></extra>"
        )
    else:
        customdata = grp[["Proventos", "Descontos"]].values
        hovertemplate = (
            "<b>%{y}</b><br>"
            "Índice: %{x:.1f}%<br>"
            "Proventos: R$ %{customdata[0]:,.2f}<br>"
            "Descontos: R$ %{customdata[1]:,.2f}<extra></extra>"
        )

    colors = ["#e74c3c" if v >= limiar else "#2ecc71" for v in grp["Índice (%)"]]
    texto  = grp["Índice (%)"].apply(lambda v: f"{v:.1f}%")

    fig = go.Figure(go.Bar(
        x=grp["Índice (%)"],
        y=grp[col],
        orientation="h",
        marker_color=colors,
        text=texto,
        textposition="outside",
        customdata=customdata,
        hovertemplate=hovertemplate
    ))

    # Linha do limiar
    fig.add_vline(
        x=limiar,
        line_dash="dash",
        line_color="#f39c12",
        annotation_text=f"  Limiar {limiar:.0f}%",
        annotation_font_color="#f39c12",
        annotation_position="top right"
    )

    alertas = (grp["Índice (%)"] >= limiar).sum()
    titulo  = f"🚨 Índice de Comprometimento por {col} — {alertas} acima do limiar"

    fig.update_layout(
        title=titulo,
        xaxis_title="Descontos / Proventos (%)",
        yaxis_title="",
        height=max(400, len(grp) * 28),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"),
        xaxis=dict(gridcolor="rgba(255,255,255,0.1)", ticksuffix="%"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.1)")
    )
    return fig, alertas, grp


# ============================================================
# LAYOUT DO DASHBOARD
# ============================================================
st.set_page_config(page_title="Ficha Financeira - RM TOTVS", page_icon="📊", layout="wide")

# Inicializa todas as chaves do session_state para evitar KeyError
_defaults = {
    "df": pd.DataFrame(),
    "param_coligada": "1",
    "param_ano": 2024,
    "executar_consulta": False,
    "conexao_ok": False,
    "servidor_base": "http://localhost:8051",
    "rm_usuario": "mestre",
    "rm_senha": "",
    "wsdl_url": "",
}
for _k, _v in _defaults.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v
st.title("📊 Ficha Financeira - RM TOTVS")
st.markdown("---")

# ============================================================
# CONFIGURAÇÕES DE CONEXÃO
# ============================================================
conexao_ok = st.session_state.get("conexao_ok", False)

with st.expander("⚙️ Configurações de Conexão", expanded=not conexao_ok):
    st.caption("Informe os dados do servidor RM para estabelecer a conexão.")
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        servidor_input = st.text_input(
            "🌐 Endereço do Servidor",
            value=st.session_state.get("servidor_base", "http://localhost:8051"),
            placeholder="Ex: http://192.168.1.10:8051",
            help="Informe apenas o endereço base, sem barras no final."
        )
    with col2:
        usuario_input = st.text_input(
            "👤 Usuário",
            value=st.session_state.get("rm_usuario", "mestre")
        )
    with col3:
        senha_input = st.text_input(
            "🔒 Senha",
            value=st.session_state.get("rm_senha", ""),
            type="password"
        )

    if st.button("💾 Salvar Configurações", use_container_width=True):
        servidor_base = servidor_input.strip().rstrip("/")
        if not servidor_base.startswith("http"):
            st.error("⚠️ O endereço do servidor deve começar com http:// ou https://")
        elif not usuario_input.strip():
            st.error("⚠️ Informe o usuário.")
        elif not senha_input.strip():
            st.error("⚠️ Informe a senha.")
        else:
            st.session_state["servidor_base"] = servidor_base
            st.session_state["wsdl_url"]      = servidor_base + WSDL_SUFIXO
            st.session_state["rm_usuario"]    = usuario_input.strip()
            st.session_state["rm_senha"]      = senha_input
            st.session_state["conexao_ok"]    = True
            # Limpa dados anteriores ao trocar conexão
            st.session_state.pop("df", None)
            st.success(f"✅ Conexão configurada! URL: `{st.session_state['wsdl_url']}`")
            st.rerun()

if conexao_ok:
    st.info(
        f"🔗 Conectado em: `{st.session_state['wsdl_url']}` "
        f"| Usuário: `{st.session_state['rm_usuario']}`"
    )

st.markdown("---")

# Bloqueia o restante do app se a conexão ainda não foi configurada
if not st.session_state.get("conexao_ok"):
    st.warning("⚠️ Configure e salve as **Configurações de Conexão** acima antes de consultar.")
    st.stop()

# ============================================================
# FORMULÁRIO DE CONSULTA
# ============================================================
st.subheader("🔍 Parâmetros da Consulta")

with st.form("form_consulta"):
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        coligada_input = st.text_input("Coligada", value="1", help="Informe o código da coligada")
    with col2:
        ano_input = st.number_input("Ano", min_value=2000, max_value=2100,
                                    value=2024, step=1, help="Informe o ano de competência")
    with col3:
        st.markdown("<br>", unsafe_allow_html=True)
        consultar = st.form_submit_button("🔎 Consultar", use_container_width=True)

if consultar:
    if not coligada_input.strip().isdigit():
        st.error("Coligada deve ser um número válido.")
        st.stop()
    st.session_state["param_coligada"] = coligada_input
    st.session_state["param_ano"] = ano_input
    st.session_state["executar_consulta"] = True

if st.session_state.get("executar_consulta"):
    st.session_state["executar_consulta"] = False
    with st.spinner(f"Buscando dados da coligada {st.session_state['param_coligada']} / ano {st.session_state['param_ano']}..."):
        st.session_state["df"] = buscar_dados(
            int(st.session_state["param_coligada"]),
            int(st.session_state["param_ano"])
        )

df: pd.DataFrame = st.session_state.get("df", pd.DataFrame())

# Se ainda não consultou ou df não tem as colunas esperadas, para aqui
if df.empty or "Ano" not in df.columns:
    st.info("👆 Preencha a Coligada e o Ano acima e clique em **Consultar** para carregar os dados.")
    st.stop()

colunas_esperadas = ["Ano", "Mês", "Nome", "Tipo Evento", "Evento", "Valor", "Empresa"]
colunas_faltando = [c for c in colunas_esperadas if c not in df.columns]
if colunas_faltando:
    st.error(f"Colunas não encontradas no retorno: {colunas_faltando}")
    st.write("Colunas recebidas:", df.columns.tolist())
    st.dataframe(df.head())
    st.stop()

st.success(f"✅ Coligada **{st.session_state['param_coligada']}** | Ano **{st.session_state['param_ano']}** | **{len(df):,}** registros carregados.")
st.markdown("---")

# ============================================================
# FILTROS
# ============================================================
st.subheader("🔎 Filtros")
col1, col2, col3, col4 = st.columns(4)

with col1:
    anos = st.multiselect("Ano", sorted(df["Ano"].unique()), default=sorted(df["Ano"].unique()))
with col2:
    tipos = st.multiselect("Tipo de Evento", df["Tipo Evento"].unique(), default=df["Tipo Evento"].unique())
with col3:
    periodos_disponiveis = sorted(df["Período"].dropna().unique().tolist())
    periodos_sel = st.multiselect("Período", periodos_disponiveis, default=periodos_disponiveis)
with col4:
    lista_funcionarios = ["Todos"] + sorted(df["Nome"].unique().tolist())
    funcionario_sel = st.selectbox("👤 Funcionário", lista_funcionarios)

mes_min = int(df["Mês"].min())
mes_max = int(df["Mês"].max())

mes_inicio, mes_fim = st.slider(
    "📅 Intervalo de Mês",
    min_value=mes_min, max_value=mes_max,
    value=(mes_min, mes_max), format="%d",
    help="Arraste para filtrar o intervalo de meses"
)
st.caption(f"Filtrando de **{MESES[mes_inicio]}** até **{MESES[mes_fim]}**")

nomes_filtro = df["Nome"].unique() if funcionario_sel == "Todos" else [funcionario_sel]

df_filtrado = df[
    df["Ano"].isin(anos) &
    df["Tipo Evento"].isin(tipos) &
    df["Período"].isin(periodos_sel) &
    df["Nome"].isin(nomes_filtro) &
    df["Mês"].between(mes_inicio, mes_fim)
]

st.markdown("---")

# ============================================================
# MÉTRICAS
# ============================================================
st.subheader("📈 Resumo")
col1, col2, col3, col4 = st.columns(4)

total_proventos = df_filtrado[df_filtrado["Tipo Evento"] == "Provento"]["Valor"].sum()
total_descontos = df_filtrado[df_filtrado["Tipo Evento"] == "Desconto"]["Valor"].sum()
saldo           = total_proventos - total_descontos

col1.metric("Total de Registros", len(df_filtrado))
col2.metric("Total Proventos",    fmt(total_proventos))
col3.metric("Total Descontos",    fmt(total_descontos))
col4.metric("Saldo Líquido",      fmt(saldo))

st.markdown("---")

# ============================================================
# GRÁFICOS
# ============================================================
st.plotly_chart(grafico_proventos_descontos_saldo(df_filtrado), use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    st.plotly_chart(grafico_evolucao_saldo(df_filtrado), use_container_width=True)
with col2:
    st.plotly_chart(grafico_ranking_eventos(df_filtrado), use_container_width=True)

tipo_valor = st.radio(
    "💰 Tipo de Valor — Gastos por Função e Seção",
    options=["Valor Bruto", "Valor Líquido"],
    horizontal=True,
    help="Selecione se os gráficos de Função e Seção exibem o valor bruto ou o valor líquido (proventos − descontos)"
)
coluna_valor = "Liquido" if tipo_valor == "Valor Líquido" else "Valor"

col1, col2 = st.columns(2)
with col1:
    st.plotly_chart(grafico_gastos_funcao(df_filtrado, coluna_valor), use_container_width=True)
with col2:
    st.plotly_chart(grafico_gastos_secao(df_filtrado, coluna_valor), use_container_width=True)

st.markdown("---")

# ============================================================
# ÍNDICE DE COMPROMETIMENTO
# ============================================================
st.subheader("🚨 Índice de Comprometimento de Descontos")
st.caption("Proporção de Descontos em relação aos Proventos. Valores acima do limiar são destacados em vermelho.")

col_limiar, col_spacer = st.columns([1, 3])
with col_limiar:
    limiar_pct = st.slider(
        "⚠️ Limiar de alerta (%)",
        min_value=10, max_value=80, value=30, step=5,
        help="Registros com índice acima deste valor serão marcados em vermelho"
    )

tabs_comp = st.tabs(["👤 Por Funcionário", "🏢 Por Seção", "👔 Por Função"])

agrupamentos = ["Nome", "Seção", "Função"]
for tab, agrup in zip(tabs_comp, agrupamentos):
    with tab:
        fig_comp, qtd_alertas, df_comp = grafico_comprometimento(df_filtrado, limiar_pct, agrup)
        if qtd_alertas > 0:
            st.warning(f"⚠️ **{qtd_alertas}** {agrup.lower()}(s) com índice de comprometimento acima de **{limiar_pct}%**")
        else:
            st.success(f"✅ Nenhum(a) {agrup.lower()} acima do limiar de **{limiar_pct}%**")
        st.plotly_chart(fig_comp, use_container_width=True)

        # Tabela resumo dos que estão em alerta
        df_alerta = df_comp[df_comp["Índice (%)"] >= limiar_pct].sort_values("Índice (%)", ascending=False)
        if not df_alerta.empty:
            with st.expander(f"📋 Ver detalhes dos {agrup.lower()}(s) em alerta"):
                df_alerta_fmt = df_alerta[[agrup, "Proventos", "Descontos", "Índice (%)"]].copy()
                df_alerta_fmt["Proventos"] = df_alerta_fmt["Proventos"].apply(fmt)
                df_alerta_fmt["Descontos"] = df_alerta_fmt["Descontos"].apply(fmt)
                df_alerta_fmt["Índice (%)"] = df_alerta_fmt["Índice (%)"].apply(lambda v: f"{v:.1f}%")
                st.dataframe(df_alerta_fmt.reset_index(drop=True), use_container_width=True)

st.markdown("---")
st.subheader("📋 Dados Detalhados")

tab1, tab2 = st.tabs(["📊 Análise Dinâmica (PyGWalker)", "📋 Tabela"])

with tab1:
    st.caption("Arraste os campos para linhas/colunas, mude o tipo de gráfico e crie seus próprios agrupamentos!")
    renderer = StreamlitRenderer(df_filtrado.sort_values(["Ano", "Mês", "Nome"]).reset_index(drop=True))
    renderer.explorer()

with tab2:
    st.dataframe(
        df_filtrado.sort_values(["Ano", "Mês", "Nome"]).reset_index(drop=True),
        use_container_width=True
    )
    csv = df_filtrado.to_csv(index=False, sep=";", decimal=",").encode("utf-8")
    st.download_button(label="⬇️ Baixar CSV", data=csv, file_name="ficha_financeira.csv", mime="text/csv")
