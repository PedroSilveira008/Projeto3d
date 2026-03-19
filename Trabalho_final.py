import pandas as pd
import numpy as np
import streamlit as st
import matplotlib.pyplot as plt
from datetime import datetime, date, timedelta
from PIL import Image
import base64
import bcrypt
import time
from streamlit_calendar import calendar
import psycopg2


st.set_page_config(page_title='Nexlab3D',page_icon='⚙', layout='centered')

# Fundo
def fundo(imagem):
    with open(imagem, 'rb') as img:
        img_e = base64.b64encode(img.read()).decode()
    st.markdown(
        f'''
        <style>
        .stApp {{
            background-image: url('data:image/png;base64,{img_e}');
            background-size: cover;
            background-position: center;
            background-repeat: no-repeat;
            background-attachment: unset;
        }}
        </style>
        ''',
        unsafe_allow_html=True
    )

# Cards
def card(titulo, info):
    st.markdown(f'''
    <div style='
        background: rgba(15, 15, 20, 0.30);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(99, 102, 241, 0.5);
        border-left: 3px solid #6366f1;
        padding: 20px;
        border-radius: 16px;
        box-shadow: 0 10px 35px rgba(0,0,0,0.6);
        margin-bottom: 20px;
    '>
        <h2 style="color:#9bbdff;">{titulo}</h2>
        <div style="color:#9bbdff;">{info}</div>
    </div>
    ''', unsafe_allow_html=True)

# Box com emoji
def box(infos, valor, icon):
    st.markdown(f'''
    <div style='
        background: rgba(15, 15, 20, 0.15);
        backdrop-filter: blur(4px);
        border: 0.8px solid rgba(192, 192, 192, 0.5);
        border-left: 2px solid #aeadc4;
        padding: 10px;
        border-radius: 14px;
        box-shadow: 0 10px 35px rgba(0,0,0,0.6);
        margin-bottom: 35px;
    '>
        <h7 style="color:#b2b1c9; margin:0;">{infos}</h7>
        <h5 style="color:#d2d1e6; margin-top:5px">{icon} {valor}</h5>
    </div>
    ''', unsafe_allow_html=True)

fundo('bg.png')


# CONEXÃO

def conectar():
    return psycopg2.connect(
        host=st.secrets["DB_HOST"],
        port=5432,
        dbname=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"]
    )

def parar_maq(): 
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute('''
            UPDATE maquinas
            SET status='Parada'
            WHERE status='Operando'
            AND fim_operando <= NOW()
            ''')
        conn.commit()

def prod_impresso():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute('''
            UPDATE produtos
            SET status_produto = 'impresso'
            WHERE status_produto = 'imprimindo'
            AND EXISTS (
                SELECT 1
                FROM maquinas m
                WHERE m.id_maq = produtos.id_maq
                AND m.fim_operando <= NOW()
            )
            ''')
        conn.commit()

prod_impresso()
parar_maq()


# SESSION STATE

if 'logado' not in st.session_state:
    st.session_state.logado = False
if 'tela_login' not in st.session_state:
    st.session_state.tela_login = 'criar'
if 'nivel' not in st.session_state:
    st.session_state.nivel = None


# FUNÇÕES

def hash_senha(senha):
    return bcrypt.hashpw(senha.encode(), bcrypt.gensalt()).decode()

def verifica_senha(senha, hash_salvo):
    return bcrypt.checkpw(senha.encode(), hash_salvo.encode())

def bd_filamento():
    with conectar() as conn:
        return pd.read_sql_query('SELECT * FROM filamentos', conn)

def bd_maquinas():
    with conectar() as conn:
        return pd.read_sql_query('SELECT * FROM maquinas', conn)

def bd_pedidos():
    query = '''
    SELECT * FROM pedidos pd
    JOIN usuarios u ON pd.id = u.id
    '''
    with conectar() as conn:
        return pd.read_sql_query(query, conn)
    
def bd_produtos():
    with conectar() as conn:
        return pd.read_sql_query('SELECT * FROM produtos', conn)

def bd_vendas():
    query = '''
    SELECT v.id_vendas, p.nome_prod, v.quantidade, v.data, v.prazo,
           v.dt_vencimento, v.plataforma, v.valor, v.status_venda
    FROM vendas v
    JOIN produtos p ON v.id_produto = p.id_produto
    '''
    with conectar() as conn:
        return pd.read_sql_query(query, conn)

def novo_filamento(id_filamento, tipo, cor, diametro, lote, marca, estoque, custo):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                '''INSERT INTO filamentos
                (id_filamento, tipo, cor, diametro, lote, marca, estoque, custo)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',
                (id_filamento, tipo, cor, diametro, lote, marca, estoque, custo)
            )
        conn.commit()

def alerta_estoque():
    with conectar() as conn:
        estq_baixo = pd.read_sql_query(
            'SELECT * FROM filamentos WHERE estoque <= 200', conn
        )
    return estq_baixo

def nova_maq(nome, horas_uso, dt_manutencao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                '''INSERT INTO maquinas
                (nome, status, horas_uso, dt_manutencao)
                VALUES (%s,%s,%s,%s)''',
                (nome, 'Parada', horas_uso, dt_manutencao)
            )
        conn.commit()

def novo_pedido(id_usuario, id_produto, cliente):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                '''
                INSERT INTO pedidos (id, id_produto, cliente)
                VALUES (%s, %s, %s)
                ''',
                (id_usuario, id_produto, cliente)
            )
        conn.commit()

def novo_produto(id_filamento, id_maq, nome_prod, tempo_imprimir, gasto_filamento, personalizacao, custo, preco):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                '''INSERT INTO produtos
                (id_filamento, id_maq, nome_prod,
                 tempo_imprimir, gasto_filamento, personalizacao,
                 custo, preco)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id_produto''',
                (id_filamento, id_maq, nome_prod,
                 tempo_imprimir, gasto_filamento,
                 personalizacao, custo, preco)
            )
            id_produto = cur.fetchone()[0]
        conn.commit()
        return id_produto

def nova_venda(id_produto, prazo=7):
    hoje = date.today()
    dt_vencimento = hoje + timedelta(days=prazo)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                '''INSERT INTO vendas
                (id_produto, quantidade, data, prazo, dt_vencimento, plataforma, valor, status_venda)
                SELECT id_produto, 1, %s, %s, %s, 'Shopee', preco, 'pendente'
                FROM produtos WHERE id_produto = %s''',
                (hoje, prazo, dt_vencimento, id_produto)
            )
        conn.commit()

def venda_enviada(id_produto):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE vendas SET status_venda = 'enviado' WHERE id_produto = %s",
                (id_produto,)
            )
        conn.commit()

def atualizar_produto(id_produto, status_produto):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'UPDATE produtos SET status_produto = %s WHERE id_produto = %s',
                (status_produto, id_produto)
            )
        conn.commit()

def buscar_produto_por_id(id_produto):
    with conectar() as conn:
        return pd.read_sql_query(
            'SELECT * FROM produtos WHERE id_produto = %s',
            conn,
            params=(id_produto,)
        )###

def buscar_produto_por_cor(cor):
    with conectar() as conn:
        return pd.read_sql_query(
            '''SELECT p.*, f.cor
               FROM produtos p
               JOIN filamentos f ON p.id_filamento = f.id_filamento
               WHERE f.cor ILIKE %s''',
            conn,
            params=(f"%{cor}%",)
        )

def producao_por_status():
    query = '''
    SELECT status_produto, COUNT(*) as quantidade
    FROM produtos
    GROUP BY status_produto
    '''
    with conectar() as conn:
        return pd.read_sql_query(query, conn)

def calendario():
    calendario_opt = {
        'initialView': 'dayGridMonth',
        'locale': 'pt-br',
        'buttonText': {
            'today': 'Hoje',
        }
    }

    with conectar() as conn:
        df = pd.read_sql_query(
            '''
            SELECT p.nome_prod, v.dt_vencimento, v.status_venda
            FROM vendas v
            JOIN produtos p ON v.id_produto = p.id_produto
            ''', conn
        )

    if df.empty:
        st.info('Nenhum evento no calendário.')
        return

    df['dt_vencimento'] = pd.to_datetime(df['dt_vencimento']).dt.strftime('%Y-%m-%d')
    eventos = []

    for _, row in df.iterrows():
        cor = 'blue'
        if row['status_venda'] == 'atrasado':
            cor = 'red'
        elif row['status_venda'] == 'enviado':
            cor = 'green'

        eventos.append({
            'title': row['nome_prod'],
            'start': row['dt_vencimento'],
            'color': cor
        })

    calendar(events=eventos, options=calendario_opt, key='Calendar')


def validar_filamento(id_filamento, tipo, cor, marca, custo):
    if id_filamento == 0:
        st.error('Informe o ID do filamento.'); return False
    if tipo == '':
        st.error('Informe o tipo do filamento.'); return False
    if cor == '':
        st.error('Informe a cor do filamento.'); return False
    if marca == '':
        st.error('Informe a marca do filamento.'); return False
    if custo == 0:
        st.error('Informe o custo do filamento.'); return False
    return True


# TELA LOGIN

if not st.session_state.logado:
    st.markdown('''
    <style>
    .login-box {
        background: rgba(15,15,20,0.94);
        backdrop-filter: blur(14px);
        border-radius: 20px;
        padding: 35px;
        max-width: 420px;
        margin: 80px auto;
        box-shadow: 0 20px 50px rgba(0,0,0,0.75);
        text-align:center;
    }
    .link {
        color:#9bbdff;
        font-size:14px;
        cursor:pointer;
    }
    </style>
    ''', unsafe_allow_html=True)

    img = Image.open('logo.png')
    st.image(img)

# CRIAR CONTA

    if st.session_state.tela_login == 'criar':
        st.markdown("<h2 style='color:#9bbdff;'>Criar conta</h2>", text_alignment='center', unsafe_allow_html=True)

        nome = st.text_input('Usuário', icon=':material/person:').strip()
        email = st.text_input('Email', placeholder='usuario@email.com', icon=':material/mail:').strip()
        senha = st.text_input('Senha', type='password', icon=':material/key:').strip()
        st.session_state.unome = nome

        if st.button('Criar conta', icon=':material/account_circle:', use_container_width=True):
            with conectar() as conn:
                existe = pd.read_sql_query(
                    'SELECT id FROM usuarios WHERE nome=%s',
                    conn,
                    params=(nome,)
                )

            if len(existe) > 0:
                st.error('Usuário já existente')

            elif nome == '' or email == '' or senha == '':
                st.error('Preencha os campos para continuar')

            else:
                with conectar() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            'INSERT INTO usuarios (nome, email, senha, nivel) VALUES (%s,%s,%s,%s)',
                            (nome, email, hash_senha(senha), 'usuario')
                        )
                    conn.commit()

                st.toast('Conta criada com sucesso!', icon='✅')

        st.markdown('<div style="margin-top:20px;"><span class="link">Já tem uma conta?</span></div>', unsafe_allow_html=True)

        if st.button('Entrar', icon=':material/login:'):
            st.session_state.tela_login = 'login'
            st.rerun()

# LOGIN

    else:
        st.markdown("<h2 style='color:#9bbdff;'>Login</h2>",
                    text_alignment='center',
                    unsafe_allow_html=True)

        nome_email = st.text_input('Nome de usuário ou E-mail', icon=':material/person:')
        senha = st.text_input('Senha', type='password', icon=':material/key:')

        if st.button('Acessar', icon=':material/input:', use_container_width=True):
            with conectar() as conn:
                user = pd.read_sql_query(
                    '''SELECT nome, email, senha, nivel
                       FROM usuarios
                       WHERE nome=%s OR email=%s''',
                    conn,
                    params=(nome_email, nome_email)
                )

            if len(user) > 0 and verifica_senha(senha, user.iloc[0]['senha']):
                st.session_state.logado = True
                st.session_state.usuario = nome_email
                st.session_state.unome = user.iloc[0]['nome']
                st.session_state.nivel = user.iloc[0]['nivel']
                time.sleep(1)
                st.rerun()
            else:
                st.error('Usuário ou senha inválidos')

        if st.button('Criar nova conta', icon=':material/account_circle:'):
            st.session_state.tela_login = 'criar'
            st.rerun()

    st.markdown('</div>', unsafe_allow_html=True)


# ABAS APÓS LOGIN

else:
    nome_abas = [':material/home: Início', ':material/box: Estoque', ':material/3d: Máquinas', ':material/deployed_code: Produtos', ':material/attach_money: Vendas', ':material/bar_chart: Gráficos',':material/calendar_month: Calendário']
    if st.session_state.nivel == 'admin':
        nome_abas.append(':material/boy: Usuários')
    abas = st.tabs(nome_abas)

# ABA 1 - INÍCIO
    with abas[0]:
        with conectar() as conn:
            vendas_hj = pd.read_sql_query(
                '''
                SELECT COUNT(*) as total FROM vendas
                WHERE DATE(data) = CURRENT_DATE
                ''', conn).iloc[0]['total']

            maq_operando = pd.read_sql_query(
                '''
                SELECT COUNT(*) as total FROM maquinas
                WHERE status = 'Operando'
                ''', conn).iloc[0]['total']

            with conn.cursor() as cur:
                cur.execute(
                    '''
                    UPDATE vendas
                    SET status_venda='atrasado'
                    WHERE status_venda!='enviado'
                    AND dt_vencimento < CURRENT_DATE
                    '''
                )
            conn.commit()

            alertas_envio = pd.read_sql_query(
                '''
                SELECT COUNT(*) AS total
                FROM vendas
                WHERE status_venda != 'enviado'
                AND dt_vencimento BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '2 days'
                ''', conn).iloc[0]['total']

            atrasados = pd.read_sql_query(
                '''
                SELECT COUNT(*) AS total
                FROM vendas
                WHERE status_venda != 'enviado'
                AND dt_vencimento < CURRENT_DATE
                ''', conn).iloc[0]['total']

            df_mes = pd.read_sql_query(
                '''
                SELECT valor FROM vendas
                WHERE data >= DATE_TRUNC('month', CURRENT_DATE)
                ''', conn)

        fat_mensal = df_mes['valor'].sum()

        st.markdown(f"<h2 style='color=white;'>Bem-vindo, {st.session_state.unome}!</h2>", unsafe_allow_html=True)
        st.caption('Sistema de Gestão – Impressora 3D')

        with st.popover(':material/menu_book: Tutorial'):
            st.caption('Acessar o tutorial')
            if st.session_state.nivel == 'admin':
                st.markdown('[Manual](https://sustentabilize.my.canva.site/tutorial-admin)')
            elif st.session_state.nivel == 'operador':
                st.markdown('[Manual](https://sustentabilize.my.canva.site/tutorial-operador)')
            else:
                st.markdown('[Manual](https://sustentabilize.my.canva.site/tutorial-usu-rio)')

        c1, c2, c3, c4 = st.columns(4)## 
        
        with c1:
            box('Vendas de hoje', vendas_hj,'💵')
        with c2:
            box('Máquinas operando', maq_operando,'🖨️')
        with c3:
            if atrasados > 0:
                box('Envios atrasados', f'{atrasados} pedido(s)', '🚨')
            elif alertas_envio > 0:
                box('Envios próximos (2 dias)', f'{alertas_envio} pedido(s)', '⚠')
            else:
                box('Envios', 'Prazo seguro', '✅')

        if st.session_state.nivel == 'admin':
            with c4:
                box('Faturamento mensal',f'R$ {fat_mensal:.2f}','📈')

        col_p, col_e, coln, col_d = st.columns([1,1.5,0.1,1])

        with col_p:
            st.markdown('**:material/account_circle: Perfil**')
            st.caption(f'**{st.session_state.unome}**')
            st.caption(f'Nível: {st.session_state.nivel}')

            if st.button(':material/logout: Sair'):
                st.session_state.logado = False
                st.session_state.tela_login = 'login'
                st.rerun()

        with col_e:
            st.markdown('**:material/release_alert: Alertas**')
            limite = 23

            with conectar() as conn:
                df_maquinas = pd.read_sql_query(
                    'SELECT id_maq, nome, dt_manutencao FROM maquinas', conn
                )

            if df_maquinas.empty:
                st.info('Nenhuma máquina cadastrada.')
            else:
                df_maquinas['dt_manutencao'] = pd.to_datetime(df_maquinas['dt_manutencao'], errors='coerce')
                hoje = pd.to_datetime(date.today())
                df_maquinas['dias_sem_manutencao'] = (hoje - df_maquinas['dt_manutencao']).dt.days

                atrasadas = df_maquinas[df_maquinas['dias_sem_manutencao'] >= limite]

                if atrasadas.empty:
                    st.success('✔ Todas as máquinas estão com manutenção em dia!')
                else:
                    st.error(f'⚠ {len(atrasadas)} máquina(s) sem manutenção há mais de {limite} dias!')
                    st.dataframe(
                        atrasadas[['id_maq','nome','dt_manutencao','dias_sem_manutencao']],
                        use_container_width=True
                    )

            df_alerta = alerta_estoque()
            if not df_alerta.empty:
                st.error(f'⚠ {len(df_alerta)} filamento(s) com estoque crítico!')
                st.dataframe(df_alerta[['id_filamento','tipo','cor','estoque']], use_container_width=True)
            else:
                st.success('✔ Estoque de filamentos em nível seguro')

        with col_d:
            st.markdown('**:material/filter_alt: Busca por Filtros**')
            menu = st.radio('⌕ O que deseja buscar', ['Produto(s) por ID','Produto(s) pro cor'], index=None)

            if menu == 'Produto(s) por ID':
                id_busca = st.number_input('Informe o ID do produto', min_value=1)

                if st.button('Filtrar por ID'):
                    resultado = buscar_produto_por_id(id_busca)

                    if len(resultado) == 0:
                        st.warning('Produto não encontrado')
                    else:
                        st.dataframe(resultado, use_container_width=True)

            if menu == 'Produto(s) pro cor':
                cor_busca = st.text_input('Informe a cor do produto')

                if st.button('Filtrar por cor'):
                    resultado = buscar_produto_por_cor(cor_busca)

                    if len(resultado) == 0:
                        st.warning('Nenhum produto encontrado com essa cor')
                    else:
                        st.dataframe(resultado, use_container_width=True)


# ABA 2 - ESTOQUE

    with abas[1]:
        aba2_1, aba2_2 = st.tabs(['Estoque de filamentos', 'Adicionar material'])

        with aba2_1:
            card('Estoque de Filamentos','Visualização completa dos materiais atualmente cadastrados no sistema.')
            st.dataframe(bd_filamento(), use_container_width=True)

        with aba2_2:
            if st.session_state.nivel == 'usuario':
                st.error('☹ Você não têm permissão para acessar esta aba')
            else:
                card('Adicionar Novo Filamento', 'Preencha corretamente os dados para registrar um novo material no estoque.')
               
                id_filamento = st.number_input('ID filamento *', min_value=1)

                with conectar() as conn:
                    id_regs = pd.read_sql_query(
                        'SELECT * FROM filamentos WHERE id_filamento = %s',
                        conn,
                        params=(id_filamento,)
                    )
                   
                with st.form('Adicionar filamento', clear_on_submit=True):

                    if not id_regs.empty:
                        st.info('Filamento já registrado, informe a quantidade a adicionar')
                        estoque = st.number_input('Quantidade à adicionar (g)', min_value=0.0, format='%.2f')
                        adc = st.form_submit_button('Adicionar ao estoque', icon=':material/add:')

                    else:
                        tipo = st.text_input('Tipo *')
                        cor = st.text_input('Cor *')
                        diametro = st.number_input('Diâmetro (mm)', min_value=0.0, format='%.2f', value=1.75)
                        lote = st.text_input('Lote')
                        marca = st.text_input('Marca *')
                        estoque = st.number_input('Quantidade à adicionar (g)', min_value=0.0, format='%.2f')
                        custo = st.number_input('Custo *', min_value=0.0, format='%.2f')
                        adc = st.form_submit_button('Adicionar ao estoque', icon=':material/add:')

                    if adc:
                        with conectar() as conn:
                            existe = pd.read_sql_query(
                                'SELECT estoque FROM filamentos WHERE id_filamento = %s',
                                conn,
                                params=(id_filamento,)
                            )
                           
                            if not existe.empty:
                                with conn.cursor() as cur:
                                    cur.execute(
                                        'UPDATE filamentos SET estoque = estoque + %s WHERE id_filamento = %s',
                                        (estoque, id_filamento)
                                    )
                                conn.commit()
                                st.success('✅ Estoque do filamento atualizado com sucesso!')

                            else:
                                if validar_filamento(id_filamento, tipo, cor, marca, custo):
                                    with conn.cursor() as cur:
                                        cur.execute(
                                            '''INSERT INTO filamentos
                                            (id_filamento, tipo, cor, diametro, lote, marca, estoque, custo)
                                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',
                                            (id_filamento, tipo, cor, diametro, lote, marca, estoque, custo)
                                        )
                                    conn.commit()
                                    st.success('✅ Filamento cadastrado com sucesso!')
                                else:
                                    st.stop()

                        st.dataframe(bd_filamento(), use_container_width=True)
                        time.sleep(1.5)
                        st.rerun()


# ABA 3 - MÁQUINAS

    with abas[2]:
        aba3_1, aba3_2 = st.tabs(['Máquinas', 'Cadastrar máquinas'])

        with aba3_1:
            card('Máquinas Cadastradas', 'Lista de impressoras 3D e equipamentos registrados no sistema.')
            maquinas = bd_maquinas()
            maq_ocupada = maquinas[maquinas['status'] == 'Operando']

            st.dataframe(maquinas, use_container_width=True)

            if maq_ocupada.empty:
                st.caption('Não há máquinas operando neste momento.')
            else:
                st.caption(f'Há {len(maq_ocupada)} máquina(s) operando neste momento...')

        with aba3_2:
            if st.session_state.nivel == 'usuario':
                st.error('☹ Você não têm permissão para acessar esta aba')
            else:
                card('Inserir nova máquina', 'Preencha as informações para cadastrar uma nova máquina.')

                with st.form('Cadastrar máquina', clear_on_submit=True):
                    nome = st.text_input('Nome da máquina')
                    horas_uso = st.number_input('Horas de uso', min_value=0.0, format='%.2f')
                    dt_manutencao = st.date_input('Data da última manutenção')
                    dt_manutencao = dt_manutencao.strftime('%Y-%m-%d')

                    adc = st.form_submit_button('Cadastrar', icon=':material/add:')

                    if adc:
                        nova_maq(nome, horas_uso, dt_manutencao)
                        st.success('Máquina adicionada!')
                        st.dataframe(bd_maquinas(), use_container_width=True)
                        time.sleep(1.5)
                        st.rerun()


# ABA 4 - PRODUTOS

    with abas[3]:
        aba4_1, aba4_2, aba4_3 = st.tabs(['Produtos cadastrados', 'Cadastrar produto', 'Ver status dos produtos'])

        with aba4_1:
            card('Produtos cadastrados', 'Lista completa de produtos disponíveis para venda.')
            st.dataframe(bd_produtos(), use_container_width=True)

        with aba4_2:
            if st.session_state.nivel == 'usuario':
                st.error('☹ Você não têm permissão para acessar esta aba')
            else:
                filamentos = bd_filamento()
                maquinas = bd_maquinas()

                card('Adicionar produto fabricado','Insira as informações para adicionar o produto ao catálogo.')

                id_filamento = st.selectbox('Filamento utilizado', filamentos['id_filamento'])
                filamento_info = filamentos[filamentos['id_filamento'] == id_filamento].iloc[0]
                st.caption(f"Tipo: {filamento_info['tipo']} | Cor: {filamento_info['cor']} | Estoque: {filamento_info['estoque']:.2f} g")
                
                id_maq = st.selectbox('Máquina utilizada', maquinas['id_maq'])
                maq_info = maquinas[maquinas['id_maq'] == id_maq].iloc[0]
                st.caption(f"Máquina: {maq_info['nome']}")

                nome_prod = st.text_input('Nome do produto')
                tempo_min = st.number_input('Tempo gasto de impressão (min)', min_value=0.0, format='%.2f')
                gasto_filamento = st.number_input('Filamento gasto (g)', min_value=0.0, format='%.2f')
                personalizacao = st.text_input('Personalização no produto')

                custo_grama = st.slider('Custo do filamento por grama (R$)', min_value=0.05, max_value=0.30, value=0.10, step=0.01)
                custo_hora = st.number_input('Custo da hora de impressão (R$)', min_value=0.0, format='%.2f', value=15.0)
                lucro = st.number_input('Porcentagem de lucro (%)', min_value=0.0, format='%.2f', value=20.0)

                custo_material = gasto_filamento * custo_grama
                custo_mao_obra = (tempo_min / 60) * custo_hora
                custo_total = custo_material + custo_mao_obra
                preco_calculado = custo_total * (1 + lucro / 100)

                st.caption(f'Custo do material: R${custo_material:.2f}')
                st.caption(f'Custo da mão de obra: R${custo_mao_obra:.2f}')
                st.caption(f'Preço sugerido com lucro: :green[R${preco_calculado:.2f}]')

                st.divider()

                with conectar() as conn:
                    usuarios = pd.read_sql_query('SELECT id, nome FROM usuarios', conn)

                st.markdown('### Dados do pedido')

                id_usuario = st.selectbox('ID do usuário responsável',usuarios['id'])
                user_info = usuarios[usuarios['id'] == id_usuario].iloc[0]
                st.caption(f"Responsável: {user_info['nome']}")

                cliente = st.text_input('Nome do cliente')

                if st.button('Cadastrar', icon=':material/add:'):
                    with conectar() as conn:
                        estoque_df = pd.read_sql_query(
                            'SELECT estoque FROM filamentos WHERE id_filamento = %s',
                            conn,
                            params=(id_filamento,)
                        )

                    if estoque_df.empty or estoque_df.iloc[0]['estoque'] < gasto_filamento:
                        st.error('❌ Estoque de filamento insuficiente')

                    else:
                        with conectar() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    'UPDATE filamentos SET estoque = estoque - %s WHERE id_filamento = %s',
                                    (gasto_filamento, id_filamento)
                                )
                            conn.commit()

                        id_produto = novo_produto(
                            id_filamento, id_maq, nome_prod,
                            tempo_min, gasto_filamento,
                            personalizacao, custo_total, preco_calculado
                        )

                        novo_pedido(id_usuario, id_produto, cliente)

                        st.success('✅ Produto salvo no catálogo!')
                        st.dataframe(bd_produtos(), use_container_width=True)
                        time.sleep(1.5)
                        st.rerun()##
                        
        with aba4_3:
            if st.session_state.nivel == 'usuario':
                st.error('☹ Você não têm permissão para acessar esta aba')
            else:
                produtos = bd_produtos()

                cols1 = st.columns(3)
                cols2 = st.columns(3)
                cols3 = st.columns(3)
               
                with cols1[0]:
                    st.markdown('✉ Caixa de Entrada')
                    cx = produtos[produtos['status_produto'] == 'caixa_de_entrada']

                    if not cx.empty:
                        st.dataframe(cx)
                        id_cx = st.selectbox('Selecione o ID', cx['id_produto'], key='cx')

                        if st.button("Mover para 'Fila de impressão'", key='cx_bt'):
                            atualizar_produto(id_cx, 'fila_impressao')
                            nova_venda(id_cx)

                            st.success('Pedido criado e movido para produção!')
                            time.sleep(1)
                            st.rerun()
                    else:
                        st.caption('Caixa de entrada vazia')


                with cols1[1]:
                    st.markdown('🗎 Fila de impressão')
                    fila = produtos[produtos['status_produto'] == 'fila_impressao']

                    if not fila.empty:
                        st.dataframe(fila)
                        id_fila = st.selectbox('Selecione o ID', fila['id_produto'], key='fila')

                        if st.button("Mover para 'Imprimindo'", key='fila_bt'):
                            with conectar() as conn:
                                tempo_imp = pd.read_sql_query(
                                    '''
                                    SELECT p.tempo_imprimir, m.id_maq, m.status
                                    FROM produtos p
                                    JOIN maquinas m ON p.id_maq = m.id_maq
                                    WHERE p.id_produto = %s
                                    ''',
                                    conn,
                                    params=(id_fila,)
                                )

                                if tempo_imp.empty:
                                    st.error('Produto ou máquina não encontrados')
                                    st.stop()

                                maq_id = tempo_imp.iloc[0]['id_maq']
                                status_maquina = tempo_imp.iloc[0]['status']
                                tempo = tempo_imp.iloc[0]['tempo_imprimir']

                                if status_maquina != 'Parada':
                                    st.error('❌ Máquina já está operando')
                                    st.stop()

                                fim = datetime.now() + timedelta(minutes=tempo)

                                with conn.cursor() as cur:
                                    cur.execute(
                                        '''
                                        UPDATE maquinas
                                        SET status='Operando', fim_operando=%s
                                        WHERE id_maq=%s
                                        ''',
                                        (fim, int(maq_id))
                                    )

                                    cur.execute(
                                        '''
                                        UPDATE produtos
                                        SET status_produto='imprimindo'
                                        WHERE id_produto=%s
                                        ''',
                                        (int(id_fila),)
                                    )

                                conn.commit()

                                st.success(f'Máquina {maq_id} agora está Operando!')

                            st.success('Produto em impressão!')
                            time.sleep(2)
                            st.rerun()
                    else:
                        st.caption('Fila de impressão vazia')
                       

                with cols1[2]:
                    st.markdown('⎙ Imprimindo')
                    impd = produtos[produtos['status_produto'] == 'imprimindo']

                    if not impd.empty:
                        st.dataframe(impd)
                        st.caption('Produto(s) em processo de impressão...')
                    else:
                        st.caption('Nenhum produto em processo de impressão')
                           

                with cols2[0]:
                    st.markdown('⎙ Impresso')
                    imp = produtos[produtos['status_produto'] == 'impresso']

                    if not imp.empty:
                        st.dataframe(imp)
                        id_imp = st.selectbox('Selecione o ID', imp['id_produto'], key='imp')

                        if st.button("Mover para 'Pós-processamento'", key='imp_bt'):
                            atualizar_produto(id_imp, 'pos_processamento')

                            st.success("Movido para 'Pós-processamento'!")
                            time.sleep(1)
                            st.rerun()
                    else:
                        st.caption('Nenhum produto impresso')
                           

                with cols2[1]:
                    st.markdown('🖌 Pós-processamento')
                    posp = produtos[produtos['status_produto'] == 'pos_processamento']

                    if not posp.empty:
                        st.dataframe(posp)
                        id_posp = st.selectbox('Selecione o ID', posp['id_produto'], key='posp')

                        if st.button("Mover para 'Pronto'", key='posp_bt'):
                            atualizar_produto(id_posp, 'pronto')

                            st.success("Movido para 'Pronto'!")
                            time.sleep(1)
                            st.rerun()
                    else:
                        st.caption('Nenhum produto aguardando pós-processamento')


                with cols2[2]:
                    st.markdown('✔ Pronto')
                    pronto = produtos[produtos['status_produto'] == 'pronto']

                    if not pronto.empty:
                        st.dataframe(pronto)
                        id_pronto = st.selectbox('Selecione o ID', pronto['id_produto'], key='pronto')

                        if st.button("Mover para 'Enviado'", key='pronto_bt'):
                            atualizar_produto(id_pronto, 'enviado')
                            venda_enviada(id_pronto)

                            st.success('Produto enviado e venda finalizada!')
                            time.sleep(1)
                            st.rerun()
                    else:
                        st.caption('Não há produtos prontos')


                with cols3[1]:
                    st.markdown('⛟ Enviado')
                    enviado = produtos[produtos['status_produto'] == 'enviado']

                    if not enviado.empty:
                        st.dataframe(enviado)
                    else:
                        st.caption('Nenhum produto enviado')


# Aba vendas        
        with abas[4]:
            if st.session_state.nivel in ['usuario', 'operador']:
                st.error('☹ Você não têm permissão para acessar esta aba')
            else:
                card('Histórico de Vendas','Registro completo de todas as vendas realizadas.')

                vendas_df = bd_vendas()
                st.dataframe(vendas_df, use_container_width=True)


# Aba gráficos                
        with abas[5]:
            if st.session_state.nivel != 'admin':
                st.error('☹ Você não têm permissão para acessar esta aba')
            else:
                card('Gráficos de eficiência','Visualização simplificada de dados em gráficos.')

                aba6_1, aba6_2, aba6_3, aba6_4 = st.tabs(
                    ['Produção por status', 'Vendas', 'Pedidos', 'Máquinas']
                )

# Produção por status
                with aba6_1:
                    df_produtos = producao_por_status()

                    if df_produtos.empty:
                        st.warning('Não há produtos cadastrados!')
                    else:
                        fig, ax = plt.subplots(figsize=(5,3))
                        fig.patch.set_alpha(0)
                        ax.set_facecolor('none')

                        ax.bar(
                            df_produtos['status_produto'],
                            df_produtos['quantidade'],
                            color='#7d77e1',
                            edgecolor='#5751b4'
                        )

                        ax.set_title('Produção por Status', color='white')
                        ax.tick_params(axis='both', colors='white')
                        st.pyplot(fig)


# Vendas 
                with aba6_2:
                    col_m, col_s = st.columns(2)

                    with conectar() as conn:
                        vendas_mes = pd.read_sql_query(
                            '''
                            SELECT TO_CHAR(data, 'MM') as mes,
                                   COUNT(*) as total
                            FROM vendas
                            GROUP BY mes
                            ORDER BY mes
                            ''', conn
                        )

                        vendas_semana = pd.read_sql_query(
                            '''
                            SELECT TO_CHAR(data, 'WW') as semana,
                                   COUNT(*) as total
                            FROM vendas
                            GROUP BY semana
                            ORDER BY semana
                            ''', conn
                        )

                    with col_m:
                        st.markdown('### Vendas mensais')

                        fig, ax = plt.subplots(figsize=(5,3))
                        fig.patch.set_alpha(0)
                        ax.set_facecolor('none')

                        ax.plot(vendas_mes['mes'], vendas_mes['total'], marker='o', color='#7d77e1')
                        ax.set_title('Faturamento por mês', color='white')
                        ax.tick_params(colors='white')
                        st.pyplot(fig)

                    with col_s:
                        st.markdown('### Vendas semanais')

                        fig, ax = plt.subplots(figsize=(5,3))
                        fig.patch.set_alpha(0)
                        ax.set_facecolor('none')

                        ax.plot(vendas_semana['semana'], vendas_semana['total'], marker='o', color='#7d77e1')
                        ax.set_title('Faturamento por semana', color='white')
                        ax.tick_params(colors='white')
                        st.pyplot(fig)


# Pedidos
                with aba6_3:
                    col_m, col_s = st.columns(2)

                    with conectar() as conn:
                        pedidos_mes = pd.read_sql_query(
                            '''
                            SELECT TO_CHAR(v.data, 'MM') as mes,
                                   COUNT(*) as total
                            FROM pedidos p
                            JOIN vendas v ON p.id_produto = v.id_produto
                            GROUP BY mes
                            ORDER BY mes
                            ''', conn
                        )

                        pedidos_semana = pd.read_sql_query(
                            '''
                            SELECT TO_CHAR(v.data, 'WW') as semana,
                                   COUNT(*) as total
                            FROM pedidos p
                            JOIN vendas v ON p.id_produto = v.id_produto
                            GROUP BY semana
                            ORDER BY semana
                            ''', conn
                        )

                    with col_m:
                        st.markdown('### Pedidos mensais')

                        fig, ax = plt.subplots(figsize=(5,3))
                        fig.patch.set_alpha(0)
                        ax.set_facecolor('none')
                        ax.bar(pedidos_mes['mes'], pedidos_mes['total'], color='#7d77e1')
                        ax.set_title('Pedidos por mês', color='white')
                        ax.tick_params(colors='white')
                        st.pyplot(fig)

                    with col_s:
                        st.markdown('### Pedidos semanais')

                        fig, ax = plt.subplots(figsize=(5,3))
                        fig.patch.set_alpha(0)
                        ax.set_facecolor('none')
                        ax.bar(pedidos_semana['semana'], pedidos_semana['total'], color='#7d77e1')
                        ax.set_title('Pedidos por semana', color='white')
                        ax.tick_params(colors='white')
                        st.pyplot(fig)


# Horas uso
                with aba6_4:

                    col_m, col_s = st.columns(2)

                    with conectar() as conn:
                        maq_mes = pd.read_sql_query(
                            '''
                            SELECT TO_CHAR(v.data, 'MM') as mes,
                                   SUM(p.tempo_imprimir)/60 as horas
                            FROM produtos p
                            JOIN vendas v ON p.id_produto = v.id_produto
                            GROUP BY mes
                            ORDER BY mes
                            ''', conn
                        )

                        maq_semana = pd.read_sql_query(
                            '''
                            SELECT TO_CHAR(v.data, 'WW') as semana,
                                   SUM(p.tempo_imprimir)/60 as horas
                            FROM produtos p
                            JOIN vendas v ON p.id_produto = v.id_produto
                            GROUP BY semana
                            ORDER BY semana
                            ''', conn
                        )

                    with col_m:
                        st.markdown('### Horas de uso por mês')

                        fig, ax = plt.subplots(figsize=(5,3))
                        fig.patch.set_alpha(0)
                        ax.set_facecolor('none')
                        ax.bar(maq_mes['mes'], maq_mes['horas'], color='#7d77e1')
                        ax.set_title('Horas trabalhadas (mês)', color='white')
                        ax.tick_params(colors='white')
                        st.pyplot(fig)

                    with col_s:
                        st.markdown('### Horas de uso por semana')

                        fig, ax = plt.subplots(figsize=(5,3))
                        fig.patch.set_alpha(0)
                        ax.set_facecolor('none')
                        ax.bar(maq_semana['semana'], maq_semana['horas'], color='#7d77e1')
                        ax.set_title('Horas trabalhadas (semana)', color='white')
                        ax.tick_params(colors='white')
                        st.pyplot(fig)


# Aba Calendário

        with abas[6]:
            card('Calendário de prazos','Registro de produtos e data limite de envio.')
            calendario()
            

# Aba usuários - exclusiva admin
        if st.session_state.nivel == 'admin':
            with abas[-1]:
                with conectar() as conn:
                    usuarios = pd.read_sql_query(
                        'SELECT id,nome,email,nivel FROM usuarios',
                        conn
                    )

                st.dataframe(usuarios, use_container_width=True)
               
                uid = st.selectbox('ID Usuário', usuarios['id'])
                user_info = usuarios[usuarios['id'] == uid].iloc[0]
                st.caption(f"Usuário: {user_info['nome']}")
                
                nivel = st.selectbox('Novo nível', ['usuario', 'operador', 'admin'])

                if st.button('Atualizar nível', icon=':material/autorenew:'):
                    with conectar() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                'UPDATE usuarios SET nivel=%s WHERE id=%s',
                                (nivel, uid)
                            )
                        conn.commit()

                    st.success('Nível atualizado com sucesso!')
                    time.sleep(1.5)
                    st.rerun()
