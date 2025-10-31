import pandas as pd
from sqlalchemy import create_engine, text  # <--- Importe o 'text'
from pathlib import Path
import sys
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

class GTFSLoader:
    def __init__(self):
        """Inicializa a conexão com o banco de dados"""
        # (O __init__ é o mesmo, vai ler o .env que criamos)
        db_user = os.getenv('DB_USER', 'gustavo') # Mudei o padrão para gustavo
        db_password = os.getenv('DB_PASSWORD', '')
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'mobility_sptrans')
        
        # Conexão sem senha se DB_PASSWORD estiver vazio
        if db_password:
            connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        else:
            connection_string = f'postgresql://{db_user}@{db_host}:{db_port}/{db_name}'

        self.engine = create_engine(connection_string)
        self.data_dir = Path('data/raw')
        
        print(f"✅ Conexão estabelecida com {db_name} como usuário {db_user}")
    
    def truncate_tables(self):
        """Limpa todas as tabelas mantendo a estrutura (O MÉTODO CORRETO)"""
        print("\n🧹 Limpando tabelas existentes...")
        
        with self.engine.connect() as conn:
            # Desabilitar foreign keys temporariamente
            conn.execute(text("SET session_replication_role = 'replica';"))
            
            tables = ['stop_times', 'trips', 'routes', 'stops', 'agency', 'calendar', 'shapes']
            for table in tables:
                conn.execute(text(f"TRUNCATE TABLE {table} CASCADE;"))
                print(f"   ✓ {table} limpa")
            
            # Reabilitar foreign keys
            conn.execute(text("SET session_replication_role = 'origin';"))
            conn.commit()
        
        print("✅ Tabelas limpas com sucesso!")
    
    def load_agency(self):
        """Carrega dados da operadora"""
        print("\n📋 Carregando agency.txt...")
        df = pd.read_csv(self.data_dir / 'agency.txt')
        df.to_sql('agency', self.engine, if_exists='append', index=False) # <-- 'append'
        print(f"   ✅ {len(df)} registros carregados")
        return len(df)
    
    def load_routes(self):
        """Carrega dados das linhas"""
        print("\n🚍 Carregando routes.txt...")
        df = pd.read_csv(self.data_dir / 'routes.txt')
        df.to_sql('routes', self.engine, if_exists='append', index=False) # <-- 'append'
        print(f"   ✅ {len(df)} linhas carregadas")
        return len(df)
    
    def load_stops(self):
        """Carrega dados das paradas"""
        print("\n📍 Carregando stops.txt...")
        df = pd.read_csv(self.data_dir / 'stops.txt')
        df.to_sql('stops', self.engine, if_exists='append', index=False) # <-- 'append'
        print(f"   ✅ {len(df)} paradas carregadas")
        return len(df)
    
    def load_trips(self):
        """Carrega dados das viagens"""
        print("\n🚌 Carregando trips.txt...")
        df = pd.read_csv(self.data_dir / 'trips.txt')
        df.to_sql('trips', self.engine, if_exists='append', index=False) # <-- 'append'
        print(f"   ✅ {len(df)} viagens carregadas")
        return len(df)
    
    def load_stop_times(self):
        """Carrega dados dos horários (em lotes para arquivos grandes)"""
        print("\n⏰ Carregando stop_times.txt...")
        file_path = self.data_dir / 'stop_times.txt'
        
        chunksize = 50000
        total_rows = 0
        
        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunksize)):
            chunk.to_sql('stop_times', self.engine, if_exists='append', index=False) # <-- 'append'
            total_rows += len(chunk)
            print(f"   Processando... {total_rows:,} registros", end='\r')
        
        print(f"\n   ✅ {total_rows:,} horários carregados")
        return total_rows
    
    def load_calendar(self):
        """Carrega dados do calendário"""
        print("\n📅 Carregando calendar.txt...")
        df = pd.read_csv(self.data_dir / 'calendar.txt')
        
        # Converter datas de YYYYMMDD para YYYY-MM-DD
        df['start_date'] = pd.to_datetime(df['start_date'], format='%Y%m%d')
        df['end_date'] = pd.to_datetime(df['end_date'], format='%Y%m%d')
        
        df.to_sql('calendar', self.engine, if_exists='append', index=False) # <-- 'append'
        print(f"   ✅ {len(df)} registros de calendário carregados")
        return len(df)
    
    def load_shapes(self):
        """Carrega dados das rotas (shapes) em lotes"""
        print("\n🗺️  Carregando shapes.txt (arquivo grande, pode demorar)...")
        file_path = self.data_dir / 'shapes.txt'
        
        chunksize = 100000
        total_rows = 0
        
        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunksize)):
            chunk.to_sql('shapes', self.engine, if_exists='append', index=False) # <-- 'append'
            total_rows += len(chunk)
            print(f"   Processando... {total_rows:,} registros", end='\r')
        
        print(f"\n   ✅ {total_rows:,} pontos de rota carregados")
        return total_rows
    
    def load_all(self):
        """Carrega todos os dados"""
        print("=" * 60)
        print("🚀 INICIANDO PROCESSO DE ETL - GTFS SPTRANS")
        print("=" * 60)
        
        stats = {}
        
        try:
            # Limpar tabelas antes de carregar
            self.truncate_tables() # <-- CHAMANDO O MÉTODO CORRETO
            
            stats['agency'] = self.load_agency()
            stats['routes'] = self.load_routes()
            stats['stops'] = self.load_stops()
            stats['trips'] = self.load_trips()
            stats['stop_times'] = self.load_stop_times()
            stats['calendar'] = self.load_calendar()
            stats['shapes'] = self.load_shapes()
            
            print("\n" + "=" * 60)
            print("✅ ETL CONCLUÍDO COM SUCESSO!")
            print("=" * 60)
            print("\n📊 Resumo:")
            for table, count in stats.items():
                print(f"   {table:15} → {count:,} registros")
            print("=" * 60)
            
            return True
            
        except Exception as e:
            print(f"\n❌ Erro durante o ETL: {e}")
            return False

if __name__ == "__main__":
    loader = GTFSLoader()
    success = loader.load_all()
    sys.exit(0 if success else 1)