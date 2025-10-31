import pandas as pd
from pathlib import Path

def check_gtfs_files():
    """
    Verifica e analisa os arquivos GTFS baixados
    """
    data_dir = Path("data/raw")
    
    print("=" * 60)
    print("🚌 VERIFICAÇÃO DOS DADOS GTFS DA SPTRANS")
    print("=" * 60)
    
    # Arquivos GTFS esperados
    expected_files = [
        'agency.txt',
        'routes.txt',
        'trips.txt',
        'stop_times.txt',
        'stops.txt',
        'shapes.txt',
        'calendar.txt',
        'calendar_dates.txt'
    ]
    
    print("\n📁 Verificando arquivos...")
    available_files = []
    
    for file in expected_files:
        file_path = data_dir / file
        if file_path.exists():
            size = file_path.stat().st_size / (1024 * 1024)  # MB
            print(f"  ✅ {file:<25} ({size:.2f} MB)")
            available_files.append(file)
        else:
            print(f"  ❌ {file:<25} (não encontrado)")
    
    if not available_files:
        print("\n⚠️  Nenhum arquivo GTFS encontrado em data/raw/")
        print("   Por favor, extraia o arquivo ZIP baixado nesta pasta.")
        return
    
    print(f"\n✅ {len(available_files)} arquivos encontrados!")
    
    # Análise rápida dos arquivos principais
    print("\n" + "=" * 60)
    print("📊 ANÁLISE RÁPIDA DOS DADOS")
    print("=" * 60)
    
    # Routes (Linhas)
    if 'routes.txt' in available_files:
        df_routes = pd.read_csv(data_dir / 'routes.txt')
        print(f"\n🚍 LINHAS DE ÔNIBUS:")
        print(f"   Total de linhas: {len(df_routes)}")
        print(f"   Colunas: {', '.join(df_routes.columns.tolist())}")
        print(f"\n   Amostra de linhas:")
        print(df_routes[['route_id', 'route_short_name', 'route_long_name']].head(3).to_string(index=False))
    
    # Stops (Paradas)
    if 'stops.txt' in available_files:
        df_stops = pd.read_csv(data_dir / 'stops.txt')
        print(f"\n📍 PONTOS DE PARADA:")
        print(f"   Total de paradas: {len(df_stops)}")
        print(f"   Colunas: {', '.join(df_stops.columns.tolist())}")
        print(f"\n   Amostra de paradas:")
        if 'stop_lat' in df_stops.columns and 'stop_lon' in df_stops.columns:
            print(df_stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']].head(3).to_string(index=False))
    
    # Trips (Viagens)
    if 'trips.txt' in available_files:
        df_trips = pd.read_csv(data_dir / 'trips.txt')
        print(f"\n🚌 VIAGENS:")
        print(f"   Total de viagens: {len(df_trips)}")
        print(f"   Colunas: {', '.join(df_trips.columns.tolist())}")
    
    # Stop Times
    if 'stop_times.txt' in available_files:
        df_stop_times = pd.read_csv(data_dir / 'stop_times.txt', nrows=100000)
        print(f"\n⏰ HORÁRIOS:")
        print(f"   Total de registros (aprox): Mais de {len(df_stop_times):,}")
        print(f"   Colunas: {', '.join(df_stop_times.columns.tolist())}")
    
    print("\n" + "=" * 60)
    print("✅ Verificação concluída!")
    print("=" * 60)

if __name__ == "__main__":
    check_gtfs_files()
