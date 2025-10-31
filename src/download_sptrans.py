import requests
import zipfile
import io
from pathlib import Path
import sys

# --- Link SPTrans ---
DOWNLOAD_URL = "http://www.sptrans.com.br/umbraco/Surface/PerfilDesenvolvedor/BaixarGTFS"

# O destino é a pasta data/raw
PROJECT_ROOT = Path(__file__).parent.parent
DEST_DIR = PROJECT_ROOT / "data" / "raw"

def download_and_unzip_gtfs():
    """
    Baixa e extrai os dados GTFS da SPTrans para a pasta data/raw.
    """
    print("=" * 60)
    print(" Iniciando download dos dados GTFS da SPTrans (Fonte Oficial)...")
    print(f"URL: {DOWNLOAD_URL}")
    print("=" * 60)

    try:
        print("Baixando... (Isso pode levar alguns segundos)")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
        }
        response = requests.get(DOWNLOAD_URL, headers=headers, timeout=30)
        
        response.raise_for_status() 
        print(" Download concluído.")

        # Garantindo que o diretório de destino exista
        DEST_DIR.mkdir(parents=True, exist_ok=True)
        print(f"Extraindo arquivos para: {DEST_DIR.resolve()}")

        # Extraindo
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(DEST_DIR)
            
            file_list = [f.filename for f in z.infolist()]
            print("\nArquivos extraídos:")
            for file in file_list:
                print(f"  -> {file}")

        print("\n" + "=" * 60)
        print(" Download e extração concluídos com sucesso!")
        print(f"Os dados estão prontos em '{DEST_DIR.relative_to(PROJECT_ROOT)}'")
        print("=" * 60)

    except requests.exceptions.RequestException as e:
        print(f"\n ERRO DE DOWNLOAD: Não foi possível baixar o arquivo.")
        print(f"Detalhe: {e}")
        sys.exit(1) # Sai com código de erro
        
    except zipfile.BadZipFile:
        print(f"\n ERRO DE EXTRAÇÃO: O arquivo baixado não é um .zip válido.")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n Ocorreu um erro inesperado:")
        print(f"Detalhe: {e}")
        sys.exit(1)

if __name__ == "__main__":
    download_and_unzip_gtfs()
