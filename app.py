import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import io
import hashlib
import sqlite3
import os

# ==============================
# CONFIGURAÇÃO
# ==============================
NOME_EMPRESA = "Maria Luiza Material de Construção"
DB_FILE = "controle.db"

st.set_page_config(
    page_title=f"{NOME_EMPRESA} - Sistema de Controle",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================
# FUNÇÕES DE BANCO DE DADOS
# ==============================
def get_connection():
    if USE_POSTGRES:
        try:
            engine = create_engine(DATABASE_URL)
            conn = engine.connect()
            return conn
        except Exception as e:
            # Isso aparece nos logs do Streamlit Cloud
            print("ERRO AO CONECTAR NO POSTGRES:", e)
            raise
    else:
        return sqlite3.connect(DATABASE_URL, check_same_thread=False)


def init_database():
    """Inicializa o banco de dados com as tabelas"""
    conn = get_connection()
    cursor = conn.cursor()

    # Tabela de usuários
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario TEXT UNIQUE NOT NULL,
            senha_hash TEXT NOT NULL,
            nome_completo TEXT,
            data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Tabela de entradas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS entradas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data DATE NOT NULL,
            codigo_produto TEXT NOT NULL,
            descricao_produto TEXT NOT NULL,
            unidade TEXT NOT NULL,
            quantidade REAL NOT NULL,
            fornecedor TEXT,
            custo_unitario REAL NOT NULL,
            custo_total REAL NOT NULL,
            nota_fiscal TEXT,
            forma_pagamento TEXT,
            observacoes TEXT,
            usuario_registro TEXT,
            data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Tabela de saídas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS saidas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data DATE NOT NULL,
            codigo_produto TEXT NOT NULL,
            descricao_produto TEXT NOT NULL,
            unidade TEXT NOT NULL,
            quantidade REAL NOT NULL,
            cliente TEXT,
            preco_unitario REAL NOT NULL,
            total_venda REAL NOT NULL,
            nota_fiscal TEXT,
            forma_pagamento TEXT,
            observacoes TEXT,
            usuario_registro TEXT,
            data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Verificar e adicionar coluna nota_fiscal em saidas se não existir
    cursor.execute("PRAGMA table_info(saidas)")
    colunas = [col[1] for col in cursor.fetchall()]
    if "nota_fiscal" not in colunas:
        cursor.execute("ALTER TABLE saidas ADD COLUMN nota_fiscal TEXT")
        conn.commit()

    # Tabela de gastos
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gastos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data DATE NOT NULL,
            categoria TEXT NOT NULL,
            descricao TEXT,
            fornecedor_beneficiario TEXT,
            valor REAL NOT NULL,
            forma_pagamento TEXT,
            observacoes TEXT,
            usuario_registro TEXT,
            data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Tabela de produtos
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS produtos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT UNIQUE NOT NULL,
            descricao TEXT NOT NULL,
            unidade TEXT NOT NULL,
            preco_sugerido REAL NOT NULL,
            estoque_minimo REAL NOT NULL,
            estoque_inicial REAL DEFAULT 0
        )
    """)

    conn.commit()

    # Inserir usuários padrão se não existirem
    cursor.execute("SELECT COUNT(*) FROM usuarios")
    if cursor.fetchone()[0] == 0:
        usuarios_padrao = [
            ("admin", hash_password("admin123"), "Administrador"),
            ("maria", hash_password("maria2024"), "Maria Luiza"),
            ("vitoria", hash_password("vitoria123"), "Vitória")
        ]
        cursor.executemany(
            "INSERT INTO usuarios (usuario, senha_hash, nome_completo) VALUES (?, ?, ?)",
            usuarios_padrao
        )
        conn.commit()

    # Inserir produtos padrão se não existirem
    cursor.execute("SELECT COUNT(*) FROM produtos")
    if cursor.fetchone()[0] == 0:
        produtos_padrao = [

        ]
        cursor.executemany(
            "INSERT INTO produtos (codigo, descricao, unidade, preco_sugerido, estoque_minimo, estoque_inicial) VALUES (?, ?, ?, ?, ?, ?)",
            produtos_padrao
        )
        conn.commit()

    conn.close()

def hash_password(password):
    """Cria hash da senha"""
    return hashlib.sha256(password.encode()).hexdigest()

def verificar_login(usuario, senha):
    """Verifica login no banco de dados"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT senha_hash, nome_completo FROM usuarios WHERE usuario = ?",
        (usuario,)
    )
    result = cursor.fetchone()
    conn.close()

    if result and result[0] == hash_password(senha):
        return True, result[1]
    return False, None

# Funções para carregar dados
def carregar_entradas():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM entradas ORDER BY data DESC", conn)
    conn.close()
    return df

def carregar_saidas():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM saidas ORDER BY data DESC", conn)
    conn.close()
    return df

def carregar_gastos():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM gastos ORDER BY data DESC", conn)
    conn.close()
    return df

def carregar_produtos():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM produtos ORDER BY codigo", conn)
    conn.close()
    return df

# Funções para inserir dados
def inserir_entrada(data, codigo, descricao, unidade, quantidade, fornecedor,
                   custo_unit, custo_total, nf, forma_pag, obs, usuario):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO entradas (data, codigo_produto, descricao_produto, unidade, 
                            quantidade, fornecedor, custo_unitario, custo_total, 
                            nota_fiscal, forma_pagamento, observacoes, usuario_registro)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (data, codigo, descricao, unidade, quantidade, fornecedor,
          custo_unit, custo_total, nf, forma_pag, obs, usuario))
    conn.commit()
    conn.close()

def inserir_saida(data, codigo, descricao, unidade, quantidade, cliente,
                 preco_unit, total, nf, forma_pag, obs, usuario):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO saidas (data, codigo_produto, descricao_produto, unidade, 
                          quantidade, cliente, preco_unitario, total_venda, 
                          nota_fiscal, forma_pagamento, observacoes, usuario_registro)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (data, codigo, descricao, unidade, quantidade, cliente,
          preco_unit, total, nf, forma_pag, obs, usuario))
    conn.commit()
    conn.close()

def inserir_gasto(data, categoria, descricao, fornecedor, valor, forma_pag, obs, usuario):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO gastos (data, categoria, descricao, fornecedor_beneficiario, 
                          valor, forma_pagamento, observacoes, usuario_registro)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (data, categoria, descricao, fornecedor, valor, forma_pag, obs, usuario))
    conn.commit()
    conn.close()

def inserir_produto(codigo, descricao, unidade, preco, est_min, est_inicial):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO produtos (codigo, descricao, unidade, preco_sugerido, 
                                estoque_minimo, estoque_inicial)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (codigo, descricao, unidade, preco, est_min, est_inicial))
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False

# Funções para excluir dados
def excluir_entrada(id_registro):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM entradas WHERE id = ?", (id_registro,))
    conn.commit()
    conn.close()

def excluir_saida(id_registro):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM saidas WHERE id = ?", (id_registro,))
    conn.commit()
    conn.close()

def excluir_gasto(id_registro):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM gastos WHERE id = ?", (id_registro,))
    conn.commit()
    conn.close()

def excluir_produto(codigo):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM produtos WHERE codigo = ?", (codigo,))
    conn.commit()
    conn.close()

def calcular_estoque_atual():
    """Calcula estoque atual baseado em produtos, entradas e saídas"""
    produtos = carregar_produtos()
    entradas = carregar_entradas()
    saidas = carregar_saidas()

    df = produtos.copy()

    # Quantidades de entradas
    if not entradas.empty:
        ent = entradas.groupby("codigo_produto")["quantidade"].sum().reset_index()
        ent.columns = ["codigo", "qtd_entradas"]
    else:
        ent = pd.DataFrame(columns=["codigo", "qtd_entradas"])

    # Quantidades de saídas
    if not saidas.empty:
        sai = saidas.groupby("codigo_produto")["quantidade"].sum().reset_index()
        sai.columns = ["codigo", "qtd_saidas"]
    else:
        sai = pd.DataFrame(columns=["codigo", "qtd_saidas"])

    df = df.merge(ent, on="codigo", how="left")
    df = df.merge(sai, on="codigo", how="left")

    df["qtd_entradas"] = df["qtd_entradas"].fillna(0)
    df["qtd_saidas"] = df["qtd_saidas"].fillna(0)
    df["estoque_atual"] = df["estoque_inicial"] + df["qtd_entradas"] - df["qtd_saidas"]

    # Calcular valor do estoque
    df["valor_estoque"] = 0.0
    for i, row in df.iterrows():
        cod = row["codigo"]
        estoque = row["estoque_atual"]

        if not entradas.empty:
            entradas_prod = entradas[entradas["codigo_produto"] == cod]
            if not entradas_prod.empty:
                custo_medio = entradas_prod["custo_unitario"].mean()
            else:
                custo_medio = row["preco_sugerido"]
        else:
            custo_medio = row["preco_sugerido"]

        df.at[i, "valor_estoque"] = estoque * custo_medio

    return df

# Inicializar banco de dados
init_database()

# ==============================
# CSS
# ==============================
st.markdown("""
    <style>
    .main { padding: 0rem 1rem; }
    h1, h2, h3, h4 { color: #FF6B35; }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        border-left: 5px solid #FF6B35;
    }
    .stock-alert {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        border-radius: 5px;
        padding: 10px;
        margin: 5px 0;
    }
    </style>
""", unsafe_allow_html=True)

# ==============================
# CONSTANTES
# ==============================
UNIDADES = ["m³", "un", "kg", "saco", "ton", "litro", "caixa", "barra", "hora"]
FORMAS_PAGAMENTO = ["À vista", "A prazo", "Cartão débito", "Cartão crédito", "PIX", "Boleto", "Cheque"]
CATEGORIAS_GASTO = [
    "Peças de carro", "Combustíveis", "Salários de funcionários",
    "Manutenção caminhões caçamba", "Manutenção retroescavadeiras",
    "Custos de depósito – Aluguel", "Custos de depósito – Luz/Água",
    "Custos de depósito – Outros", "Seguros", "Impostos"
]

# ==============================
# AUTENTICAÇÃO
# ==============================
def tela_login():
    st.markdown(f"""
        <div style='text-align: center; padding: 50px;'>
            <h1 style='color: #FF6B35;'>🚛 {NOME_EMPRESA}</h1>
            <h3>Sistema de Controle de Materiais</h3>
        </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.subheader("🔐 Login")
        usuario = st.text_input("Usuário", key="login_user")
        senha = st.text_input("Senha", type="password", key="login_pass")

        if st.button("Entrar", use_container_width=True, type="primary"):
            sucesso, nome_completo = verificar_login(usuario, senha)
            if sucesso:
                st.session_state.autenticado = True
                st.session_state.usuario_logado = usuario
                st.session_state.nome_completo = nome_completo
                st.success(f"Bem-vindo(a), {nome_completo}!")
                st.rerun()
            else:
                st.error("❌ Usuário ou senha incorretos!")

        st.info("""
        **Usuários de teste:**
        - admin / admin123
        - maria / maria2024
        - vitoria / vitoria123
        """)

if 'autenticado' not in st.session_state:
    st.session_state.autenticado = False

if not st.session_state.autenticado:
    tela_login()
    st.stop()

# ==============================
# HEADER
# ==============================
col_h1, col_h2 = st.columns([4, 1])

with col_h1:
    st.title(f"🚛 {NOME_EMPRESA} – Sistema de Controle")

with col_h2:
    st.markdown(f"**{st.session_state.nome_completo}**")
    if st.button("🚪 Sair", use_container_width=True):
        st.session_state.autenticado = False
        st.rerun()

st.markdown("---")

# ==============================
# SIDEBAR
# ==============================
with st.sidebar:
    st.markdown(f"### 🚚 {NOME_EMPRESA}")
    st.markdown("#### Painel de Controle")
    st.markdown("---")

    st.subheader("📅 Período")
    hoje = datetime.now().date()
    data_inicial = st.date_input("Data inicial", value=hoje.replace(day=1), format="DD/MM/YYYY")
    data_final = st.date_input("Data final", value=hoje, format="DD/MM/YYYY")

    st.markdown("---")
    st.subheader("📥 Exportar")

    if st.button("📊 Gerar Excel", use_container_width=True):
        output = io.BytesIO()

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            carregar_entradas().to_excel(writer, sheet_name="ENTRADAS", index=False)
            carregar_saidas().to_excel(writer, sheet_name="SAIDAS", index=False)
            carregar_gastos().to_excel(writer, sheet_name="GASTOS", index=False)
            carregar_produtos().to_excel(writer, sheet_name="PRODUTOS", index=False)
            calcular_estoque_atual().to_excel(writer, sheet_name="ESTOQUE", index=False)

        output.seek(0)

        st.download_button(
            label="⬇️ Baixar Excel",
            data=output,
            file_name=f"MLT_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

# ==============================
# CARREGAR DADOS
# ==============================
df_entradas = carregar_entradas()
df_saidas = carregar_saidas()
df_gastos = carregar_gastos()
df_produtos = carregar_produtos()
df_estoque = calcular_estoque_atual()

# ==============================
# TABS
# ==============================
tab_dash, tab_ent, tab_sai, tab_gas, tab_prod, tab_est, tab_cmp, tab_rel = st.tabs([
    "📊 Dashboard", "📦 Entradas", "🚚 Saídas", "💸 Gastos",
    "📋 Produtos", "📦 Estoque", "🧾 Compras/Vendas (Notas)", "📈 Relatórios"
])

# ==================== DASHBOARD ====================
with tab_dash:
    st.header("📊 Dashboard Executivo")

    def filtrar_periodo(df, col_data):
        if df.empty:
            return df
        df2 = df.copy()
        df2[col_data] = pd.to_datetime(df2[col_data]).dt.date
        return df2[(df2[col_data] >= data_inicial) & (df2[col_data] <= data_final)]

    ent_periodo = filtrar_periodo(df_entradas, "data")
    sai_periodo = filtrar_periodo(df_saidas, "data")
    gas_periodo = filtrar_periodo(df_gastos, "data")

    total_vendas = sai_periodo["total_venda"].sum() if not sai_periodo.empty else 0
    total_compras = ent_periodo["custo_total"].sum() if not ent_periodo.empty else 0
    total_despesas = gas_periodo["valor"].sum() if not gas_periodo.empty else 0
    lucro_bruto = total_vendas - total_compras
    lucro_liquido = lucro_bruto - total_despesas
    valor_estoque = df_estoque["valor_estoque"].sum()

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("💰 Vendas", f"R$ {total_vendas:,.2f}")
    c2.metric("🛒 Compras", f"R$ {total_compras:,.2f}")
    c3.metric("💸 Despesas", f"R$ {total_despesas:,.2f}")
    c4.metric("📈 Lucro Bruto", f"R$ {lucro_bruto:,.2f}")
    c5.metric("✅ Lucro Líquido", f"R$ {lucro_liquido:,.2f}")
    c6.metric("📦 Estoque", f"R$ {valor_estoque:,.2f}")

    st.markdown("---")

    # Alertas de estoque
    alertas = df_estoque[df_estoque["estoque_atual"] <= df_estoque["estoque_minimo"]]

    if not alertas.empty:
        st.error(f"🚨 {len(alertas)} produto(s) com estoque crítico!")
        for _, row in alertas.iterrows():
            st.markdown(f"""
                <div class="stock-alert">
                <b>{row['descricao']}</b><br>
                Estoque: {row['estoque_atual']:.2f} {row['unidade']} | 
                Mínimo: {row['estoque_minimo']:.2f}
                </div>
            """, unsafe_allow_html=True)
    else:
        st.success("✅ Estoque em níveis adequados!")

    st.markdown("---")

    col_g1, col_g2 = st.columns(2)

    with col_g1:
        st.subheader("💰 Análise Financeira")
        df_fin = pd.DataFrame({
            "Tipo": ["Vendas", "Compras", "Despesas", "Lucro Líquido"],
            "Valor": [total_vendas, total_compras, total_despesas, lucro_liquido]
        })
        fig_fin = px.bar(df_fin, x="Tipo", y="Valor", color="Tipo", text="Valor")
        fig_fin.update_traces(texttemplate="R$ %{y:,.2f}", textposition="outside")
        fig_fin.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_fin, use_container_width=True)

    with col_g2:
        st.subheader("📦 Valor em Estoque")
        df_e = df_estoque[df_estoque["estoque_atual"] > 0]
        if not df_e.empty:
            fig_est = px.bar(df_e, x="descricao", y="valor_estoque", text="valor_estoque")
            fig_est.update_traces(texttemplate="R$ %{y:,.2f}", textposition="outside")
            fig_est.update_layout(height=400, showlegend=False, xaxis_tickangle=45)
            st.plotly_chart(fig_est, use_container_width=True)

# ==================== ENTRADAS ====================
with tab_ent:
    st.header("📦 Entradas de Mercadorias")

    with st.expander("➕ Nova Entrada", expanded=False):
        with st.form("form_entrada"):
            c1, c2, c3 = st.columns(3)

            with c1:
                data = st.date_input("Data", value=datetime.now(), format="DD/MM/YYYY")
                cod = st.selectbox("Código", options=[""] + df_produtos["codigo"].tolist())

                if cod:
                    prod = df_produtos[df_produtos["codigo"] == cod].iloc[0]
                    desc_default = prod["descricao"]
                    un_default = prod["unidade"]
                else:
                    desc_default = ""
                    un_default = UNIDADES[0]

                desc = st.text_input("Descrição", value=desc_default)

            with c2:
                unidade = st.selectbox("Unidade", options=UNIDADES, index=UNIDADES.index(un_default))
                qtd = st.number_input("Quantidade", min_value=0.01, value=1.0, step=0.01)
                fornecedor = st.text_input("Fornecedor")

            with c3:
                custo_unit = st.number_input("Custo Unitário (R$)", min_value=0.0, value=0.0, step=0.01)
                tem_nf = st.selectbox("Tem Nota Fiscal?", ["Sim", "Não"])
                if tem_nf == "Sim":
                    nf = st.text_input("Número da Nota Fiscal")
                else:
                    nf = "SEM NOTA"
                    st.info("Registrado como: SEM NOTA")
                forma_pag = st.selectbox("Forma Pagamento", options=FORMAS_PAGAMENTO)
                obs = st.text_area("Observações", height=60)

            if st.form_submit_button("💾 Salvar", use_container_width=True):
                if not cod:
                    st.error("Selecione um produto!")
                else:
                    custo_total = qtd * custo_unit
                    inserir_entrada(data, cod, desc, unidade, qtd, fornecedor,
                                  custo_unit, custo_total, nf, forma_pag, obs,
                                  st.session_state.usuario_logado)
                    st.success("✅ Entrada registrada!")
                    st.rerun()

    st.subheader("📋 Histórico de Entradas")

    if not df_entradas.empty:
        for _, row in df_entradas.head(20).iterrows():
            col_data, col_delete = st.columns([10, 1])
            with col_data:
                nota_info = f"NF: {row['nota_fiscal']}" if row['nota_fiscal'] else "SEM NOTA"
                st.write(f"**ID {row['id']}** - {row['data']} - {row['descricao_produto']} - R$ {row['custo_total']:.2f} - {nota_info}")
            with col_delete:
                if st.button("🗑️", key=f"del_ent_{row['id']}"):
                    excluir_entrada(row['id'])
                    st.success("Excluído!")
                    st.rerun()

        st.dataframe(df_entradas, use_container_width=True, height=300)
    else:
        st.info("Nenhuma entrada registrada.")

# ==================== SAÍDAS ====================
with tab_sai:
    st.header("🚚 Saídas de Mercadorias")

    with st.expander("➕ Nova Saída", expanded=False):
        with st.form("form_saida"):
            c1, c2, c3 = st.columns(3)

            with c1:
                data = st.date_input("Data", value=datetime.now(), format="DD/MM/YYYY")
                cod = st.selectbox("Código", options=[""] + df_produtos["codigo"].tolist())

                if cod:
                    prod = df_produtos[df_produtos["codigo"] == cod].iloc[0]
                    desc_default = prod["descricao"]
                    un_default = prod["unidade"]
                    preco_sug = prod["preco_sugerido"]
                    est_disp = df_estoque[df_estoque["codigo"] == cod]["estoque_atual"].iloc[0]
                else:
                    desc_default = ""
                    un_default = UNIDADES[0]
                    preco_sug = 0.0
                    est_disp = 0.0

                st.info(f"📦 Estoque: {est_disp:.2f}")
                desc = st.text_input("Descrição", value=desc_default)

            with c2:
                unidade = st.selectbox("Unidade", options=UNIDADES, index=UNIDADES.index(un_default))
                qtd = st.number_input("Quantidade", min_value=0.01, value=1.0, step=0.01)
                cliente = st.text_input("Cliente")

            with c3:
                preco_unit = st.number_input("Preço Unitário (R$)", min_value=0.0, value=float(preco_sug), step=0.01)
                tem_nf = st.selectbox("Tem Nota Fiscal?", ["Sim", "Não"])
                if tem_nf == "Sim":
                    nf = st.text_input("Número da Nota Fiscal")
                else:
                    nf = "SEM NOTA"
                    st.info("Registrado como: SEM NOTA")
                forma_pag = st.selectbox("Forma Pagamento", options=FORMAS_PAGAMENTO)
                obs = st.text_area("Observações", height=60)

            if st.form_submit_button("💾 Salvar", use_container_width=True):
                if not cod:
                    st.error("Selecione um produto!")
                elif qtd > est_disp:
                    st.error(f"Estoque insuficiente! Disponível: {est_disp:.2f}")
                else:
                    total = qtd * preco_unit
                    inserir_saida(data, cod, desc, unidade, qtd, cliente,
                                preco_unit, total, nf, forma_pag, obs,
                                st.session_state.usuario_logado)
                    st.success("✅ Venda registrada!")
                    st.rerun()

    st.subheader("📋 Histórico de Vendas")

    if not df_saidas.empty:
        for _, row in df_saidas.head(20).iterrows():
            col_data, col_delete = st.columns([10, 1])
            with col_data:
                nota_info = f"NF: {row['nota_fiscal']}" if 'nota_fiscal' in row and row['nota_fiscal'] else "SEM NOTA"
                st.write(f"**ID {row['id']}** - {row['data']} - {row['cliente']} - R$ {row['total_venda']:.2f} - {nota_info}")
            with col_delete:
                if st.button("🗑️", key=f"del_sai_{row['id']}"):
                    excluir_saida(row['id'])
                    st.success("Excluído!")
                    st.rerun()

        st.dataframe(df_saidas, use_container_width=True, height=300)
    else:
        st.info("Nenhuma venda registrada.")

# ==================== GASTOS ====================
with tab_gas:
    st.header("💸 Gastos Operacionais")

    with st.expander("➕ Novo Gasto", expanded=False):
        with st.form("form_gasto"):
            c1, c2 = st.columns(2)

            with c1:
                data = st.date_input("Data", value=datetime.now(), format="DD/MM/YYYY")
                categoria = st.selectbox("Categoria", options=CATEGORIAS_GASTO)
                desc = st.text_input("Descrição")

            with c2:
                forn = st.text_input("Fornecedor/Beneficiário")
                valor = st.number_input("Valor (R$)", min_value=0.0, value=0.0, step=0.01)
                forma_pag = st.selectbox("Forma Pagamento", options=FORMAS_PAGAMENTO)
                obs = st.text_area("Observações", height=60)

            if st.form_submit_button("💾 Salvar", use_container_width=True):
                inserir_gasto(data, categoria, desc, forn, valor, forma_pag, obs,
                            st.session_state.usuario_logado)
                st.success("✅ Gasto registrado!")
                st.rerun()

    st.subheader("📋 Histórico de Gastos")

    if not df_gastos.empty:
        for _, row in df_gastos.head(20).iterrows():
            col_data, col_delete = st.columns([10, 1])
            with col_data:
                st.write(f"**ID {row['id']}** - {row['data']} - {row['categoria']} - R$ {row['valor']:.2f}")
            with col_delete:
                if st.button("🗑️", key=f"del_gas_{row['id']}"):
                    excluir_gasto(row['id'])
                    st.success("Excluído!")
                    st.rerun()

        st.dataframe(df_gastos, use_container_width=True, height=300)
    else:
        st.info("Nenhum gasto registrado.")

# ==================== PRODUTOS ====================
with tab_prod:
    st.header("📋 Cadastro de Produtos")

    with st.expander("➕ Novo Produto", expanded=False):
        with st.form("form_produto"):
            c1, c2, c3, c4 = st.columns(4)

            with c1:
                cod = st.text_input("Código")
            with c2:
                desc = st.text_input("Descrição")
            with c3:
                un = st.selectbox("Unidade", options=UNIDADES)
            with c4:
                preco = st.number_input("Preço Sugerido (R$)", min_value=0.0, value=0.0, step=0.01)

            c5, c6 = st.columns(2)
            with c5:
                est_min = st.number_input("Estoque Mínimo", min_value=0.0, value=0.0, step=1.0)
            with c6:
                est_inicial = st.number_input("Estoque Inicial", min_value=0.0, value=0.0, step=1.0)

            if st.form_submit_button("💾 Salvar", use_container_width=True):
                if inserir_produto(cod, desc, un, preco, est_min, est_inicial):
                    st.success("✅ Produto cadastrado!")
                    st.rerun()
                else:
                    st.error("Código já existe!")

    st.subheader("📦 Produtos Cadastrados")

    if not df_produtos.empty:
        for _, row in df_produtos.iterrows():
            col_data, col_delete = st.columns([10, 1])
            with col_data:
                st.write(f"**{row['codigo']}** - {row['descricao']} - {row['unidade']} - R$ {row['preco_sugerido']:.2f}")
            with col_delete:
                if st.button("🗑️", key=f"del_prod_{row['codigo']}"):
                    excluir_produto(row['codigo'])
                    st.success("Produto excluído!")
                    st.rerun()

        st.dataframe(df_produtos, use_container_width=True, height=300)
    else:
        st.info("Nenhum produto cadastrado.")

# ==================== ESTOQUE ====================
with tab_est:
    st.header("📦 Estoque Atual")

    if not df_estoque.empty:
        st.dataframe(df_estoque, use_container_width=True, height=400)
        st.markdown(f"### 💰 Valor Total: **R$ {df_estoque['valor_estoque'].sum():,.2f}**")
    else:
        st.info("Sem produtos em estoque.")

# ==================== COMPRAS/VENDAS COM E SEM NOTA ====================
with tab_cmp:
    st.header("🧾 Compras e Vendas – Com e Sem Nota Fiscal")

    # Garantir coluna nota_fiscal em entradas
    df_entradas_aux = df_entradas.copy()
    if "nota_fiscal" not in df_entradas_aux.columns:
        df_entradas_aux["nota_fiscal"] = ""

    # Criar flag com/sem nota
    df_entradas_aux["tem_nota"] = df_entradas_aux["nota_fiscal"].apply(
        lambda x: "Com nota" if isinstance(x, str) and x.strip() not in ["", "SEM NOTA", "sem nota", "Sem nota"] else "Sem nota"
    )

    # Garantir coluna nota_fiscal em saídas
    df_saidas_aux = df_saidas.copy()
    if "nota_fiscal" not in df_saidas_aux.columns:
        df_saidas_aux["nota_fiscal"] = ""

    df_saidas_aux["tem_nota"] = df_saidas_aux["nota_fiscal"].apply(
        lambda x: "Com nota" if isinstance(x, str) and x.strip() not in ["", "SEM NOTA", "sem nota", "Sem nota"] else "Sem nota"
    )

    # Filtro por período
    def filtrar_periodo(df, col_data):
        if df.empty:
            return df
        df2 = df.copy()
        df2[col_data] = pd.to_datetime(df2[col_data]).dt.date
        return df2[(df2[col_data] >= data_inicial) & (df2[col_data] <= data_final)]

    ent_periodo = filtrar_periodo(df_entradas_aux, "data")
    sai_periodo = filtrar_periodo(df_saidas_aux, "data")

    col_top1, col_top2 = st.columns(2)

    # ================= COMPRAS (ENTRADAS) =================
    with col_top1:
        st.subheader("📥 Compras (Entradas)")

        compras_com_nota = ent_periodo[ent_periodo["tem_nota"] == "Com nota"]
        compras_sem_nota = ent_periodo[ent_periodo["tem_nota"] == "Sem nota"]

        total_com_nota = compras_com_nota["custo_total"].sum() if not compras_com_nota.empty else 0
        total_sem_nota = compras_sem_nota["custo_total"].sum() if not compras_sem_nota.empty else 0

        c1, c2 = st.columns(2)
        c1.metric("Com nota fiscal", f"R$ {total_com_nota:,.2f}")
        c2.metric("Sem nota fiscal", f"R$ {total_sem_nota:,.2f}")

        df_comp = pd.DataFrame({
            "Tipo": ["Com nota", "Sem nota"],
            "Valor": [total_com_nota, total_sem_nota]
        })
        fig_comp = px.bar(df_comp, x="Tipo", y="Valor", text="Valor", color="Tipo", color_discrete_sequence=["#3498db", "#e67e22"])
        fig_comp.update_traces(texttemplate="R$ %{y:,.2f}", textposition="outside")
        fig_comp.update_layout(height=350, showlegend=False)
        st.plotly_chart(fig_comp, use_container_width=True)

    # ================= VENDAS (SAÍDAS) =================
    with col_top2:
        st.subheader("🚚 Vendas (Saídas)")

        vendas_com_nota = sai_periodo[sai_periodo["tem_nota"] == "Com nota"]
        vendas_sem_nota = sai_periodo[sai_periodo["tem_nota"] == "Sem nota"]

        total_vendas_com_nota = vendas_com_nota["total_venda"].sum() if not vendas_com_nota.empty else 0
        total_vendas_sem_nota = vendas_sem_nota["total_venda"].sum() if not vendas_sem_nota.empty else 0

        c1, c2 = st.columns(2)
        c1.metric("Com nota fiscal", f"R$ {total_vendas_com_nota:,.2f}")
        c2.metric("Sem nota fiscal", f"R$ {total_vendas_sem_nota:,.2f}")

        df_vend = pd.DataFrame({
            "Tipo": ["Com nota", "Sem nota"],
            "Valor": [total_vendas_com_nota, total_vendas_sem_nota]
        })
        fig_vend = px.bar(df_vend, x="Tipo", y="Valor", text="Valor", color="Tipo", color_discrete_sequence=["#27ae60", "#e74c3c"])
        fig_vend.update_traces(texttemplate="R$ %{y:,.2f}", textposition="outside")
        fig_vend.update_layout(height=350, showlegend=False)
        st.plotly_chart(fig_vend, use_container_width=True)

    st.markdown("---")

    # ================= ESTOQUE DERIVADO DAS ENTRADAS COM/SEM NOTA =================
    st.subheader("📦 Estoque ligado a Compras com/sem Nota")

    # Quantidade total comprada com e sem nota por produto
    if not ent_periodo.empty:
        comp_por_prod = ent_periodo.groupby(["codigo_produto", "descricao_produto", "unidade", "tem_nota"]).agg({
            "quantidade": "sum",
            "custo_total": "sum"
        }).reset_index()
        comp_por_prod = comp_por_prod.rename(columns={"quantidade": "qtd_comprada", "custo_total": "valor_comprado"})

        st.dataframe(comp_por_prod, use_container_width=True)
    else:
        st.info("Sem compras no período para analisar com/sem nota.")

# ==================== RELATÓRIOS ====================
with tab_rel:
    st.header("📈 Relatórios e Análises")

    st.subheader("🏆 Top Produtos por Faturamento")

    if not df_saidas.empty:
        top = df_saidas.groupby("descricao_produto")["total_venda"].sum().reset_index()
        top = top.sort_values("total_venda", ascending=False).head(10)

        fig_top = px.bar(top, x="descricao_produto", y="total_venda", text="total_venda")
        fig_top.update_traces(texttemplate="R$ %{y:,.2f}", textposition="outside")
        fig_top.update_layout(height=450, showlegend=False, xaxis_tickangle=45)
        st.plotly_chart(fig_top, use_container_width=True)
    else:
        st.info("Sem vendas para análise.")

st.markdown(f"""
    <div style="text-align: center; color: #7f8c8d; margin-top: 2rem;">
        Sistema de Controle – {NOME_EMPRESA}<br>
        Usuário: {st.session_state.nome_completo} | Dados salvos permanentemente
    </div>
""", unsafe_allow_html=True)
