import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# --- Configuração da Página e Conexão ---
st.set_page_config(layout="wide")
st.title("Análise da Mobilidade Urbana - SPTrans 🚍")
# Carregar variáveis de ambiente (do arquivo .env)
load_dotenv() 
DB_USER = os.getenv('DB_USER', 'gustavo')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'mobility_sptrans')

# String de conexão (lida com senha vazia)
if DB_PASSWORD:
    connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
else:
    connection_string = f'postgresql://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Usamos @st.cache_resource para a engine
@st.cache_resource
def get_engine():
    print("Criando engine de conexão...")
    return create_engine(connection_string)

engine = get_engine()


# --- Carregamento de Dados com Cache ---
# O @st.cache_data garantiu que só vamos rodar isso uma vez
@st.cache_data
def carregar_dados():
    print("Carregando dados do banco...")
    routes_query = "SELECT * FROM routes;"
    trips_query = "SELECT route_id, trip_id, shape_id FROM trips;"
    stops_query = "SELECT stop_id, stop_name, stop_lat, stop_lon FROM stops;"
    stoptimes_query = "SELECT stop_id, trip_id, departure_time FROM stop_times;"
    shapes_query = "SELECT shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence FROM shapes;"
    
    # Usamos a engine cacheada
    df_routes = pd.read_sql(routes_query, engine)
    df_trips = pd.read_sql(trips_query, engine)
    df_stops = pd.read_sql(stops_query, engine)
    df_stop_times = pd.read_sql(stoptimes_query, engine)
    df_shapes = pd.read_sql(shapes_query, engine)
    print("Dados carregados.")
    return df_routes, df_trips, df_stops, df_stop_times, df_shapes

# Carregar todos os dados
with st.spinner('Carregando dados do banco... Isso pode levar um minuto.'):
    df_routes, df_trips, df_stops, df_stop_times, df_shapes = carregar_dados()


# --- Filtrar Dados de Ônibus (Lógica de Negócio) ---
@st.cache_data
def processar_dados_onibus(_df_routes, _df_trips, _df_stops, _df_stop_times):
    print("Processando dados de ônibus...")
    # Filtro de ônibus
    ids_onibus = _df_routes[_df_routes['route_type'] == 3]['route_id']
    df_trips_onibus = _df_trips[_df_trips['route_id'].isin(ids_onibus)]
    viagens_onibus_ids = df_trips_onibus['trip_id'].unique()
    df_stoptimes_onibus = _df_stop_times[_df_stop_times['trip_id'].isin(viagens_onibus_ids)]

    # Contagem de viagens por linha
    contagem_viagens = df_trips_onibus.groupby('route_id')['trip_id'].count().reset_index()
    contagem_viagens = contagem_viagens.rename(columns={'trip_id': 'total_viagens'})
    df_linhas_populares = pd.merge(_df_routes, contagem_viagens, on='route_id', how='inner')
    df_linhas_populares = df_linhas_populares.sort_values(by='total_viagens', ascending=False)
    
    # Contagem de passagens por parada
    contagem_paradas = df_stoptimes_onibus.groupby('stop_id')['trip_id'].count().reset_index()
    contagem_paradas = contagem_paradas.rename(columns={'trip_id': 'total_passagens'})
    df_paradas_populares = pd.merge(_df_stops, contagem_paradas, on='stop_id', how='inner')
    df_paradas_populares = df_paradas_populares.sort_values(by='total_passagens', ascending=False)
    
    # Análise de Pico vs. Vale
    df_stoptimes_onibus = df_stoptimes_onibus.copy()
    df_stoptimes_onibus['hora_str'] = df_stoptimes_onibus['departure_time'].str.slice(0, 2)
    df_stoptimes_onibus['hora_int'] = pd.to_numeric(df_stoptimes_onibus['hora_str'])
    df_stoptimes_onibus['hora_do_dia'] = df_stoptimes_onibus['hora_int'] % 24
    partidas_por_hora = df_stoptimes_onibus.groupby('hora_do_dia')['trip_id'].count().reset_index()
    partidas_por_hora = partidas_por_hora.rename(columns={'trip_id': 'total_partidas'})
    
    print("Processamento concluído.")
    return df_linhas_populares, df_paradas_populares, partidas_por_hora

# Processar os dados
with st.spinner('Processando dados de análise...'):
    df_linhas, df_paradas, df_horarios = processar_dados_onibus(df_routes, df_trips, df_stops, df_stop_times)


# --- Layout do Dashboard - Métricas Principais ---
st.header("Visão Geral do Sistema de Ônibus")
col1, col2, col3 = st.columns(3)
col1.metric("Linhas de Ônibus", len(df_linhas))
col2.metric("Paradas de Ônibus", len(df_paradas))
col3.metric("Total de Partidas Analisadas", df_horarios['total_partidas'].sum())

# --- Análise de Pico vs. Vale ---
st.header("Distribuição de Partidas ao Longo do Dia")
fig_horarios = px.bar(
    df_horarios,
    x='hora_do_dia', y='total_partidas',
    title='Partidas de Ônibus por Hora (Pico vs. Vale)',
    labels={'hora_do_dia': 'Hora do Dia', 'total_partidas': 'Total de Partidas'}
)
fig_horarios.update_layout(xaxis = dict(tickmode = 'linear', tick0 = 0, dtick = 1))
st.plotly_chart(fig_horarios, use_container_width=True)


# --- Análises de Linhas e Paradas ---
st.header("Linhas e Paradas Mais Movimentadas")
col_linhas, col_paradas = st.columns(2)

with col_linhas:
    st.subheader("Top 15 Linhas por Viagens")
    st.dataframe(df_linhas[['route_short_name', 'route_long_name', 'total_viagens']].head(15))

with col_paradas:
    st.subheader("Top 15 Paradas por Passagens")
    st.dataframe(df_paradas[['stop_name', 'total_passagens']].head(15))


# --- Análise Geoespacial Interativa ---
st.header("Análise Geoespacial")
# Opção de Mapa (Heatmap ou Traçado)
mapa_tipo = st.radio("Selecione a visualização do mapa:", ('Mapa de Calor das Paradas', 'Traçado de Linha Específica'))

if mapa_tipo == 'Mapa de Calor das Paradas':
    st.subheader("Mapa de Calor Interativo (Top 1000 Paradas)")
    
    df_top_1000 = df_paradas.head(1000)
    fig_heatmap = px.scatter_mapbox(
        df_top_1000, 
        lat="stop_lat", lon="stop_lon", 
        color="total_passagens", size="total_passagens",
        hover_name="stop_name",
        color_continuous_scale=px.colors.sequential.YlOrRd,
        size_max=15, zoom=10,
        center={"lat": -23.55, "lon": -46.64},
        mapbox_style="open-street-map"
    )
    fig_heatmap.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    st.plotly_chart(fig_heatmap, use_container_width=True)

else:
    st.subheader("Traçado de Linha Específica")
    
    # Filtro interativo para selecionar a linha
    lista_linhas = df_linhas['route_short_name'].unique()
    linha_selecionada = st.selectbox("Selecione uma linha:", lista_linhas)
    
    if linha_selecionada:
        # Lógica para encontrar o traçado
        route_id_alvo = df_routes[df_routes['route_short_name'] == linha_selecionada]['route_id'].values[0]
        shape_id_alvo = df_trips[df_trips['route_id'] == route_id_alvo]['shape_id'].values[0]
        
        df_trajeto = df_shapes[df_shapes['shape_id'] == shape_id_alvo].copy()
        df_trajeto = df_trajeto.sort_values(by='shape_pt_sequence')
        
        print(f"Plotando traçado para {linha_selecionada}...")
        
        fig_linha = px.line_mapbox(
            df_trajeto,
            lat="shape_pt_lat", lon="shape_pt_lon", 
            zoom=11,
            center={"lat": df_trajeto['shape_pt_lat'].mean(), "lon": df_trajeto['shape_pt_lon'].mean()},
            mapbox_style="open-street-map"
        )
        fig_linha.update_traces(line=dict(color='red', width=3))
        fig_linha.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig_linha, use_container_width=True)
