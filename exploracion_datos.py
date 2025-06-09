import pandas as pd
import os
from pathlib import Path
import glob
import numpy as np
from datetime import datetime
import warnings
import time
import unicodedata
warnings.filterwarnings('ignore')

class DataExplorer:
    def __init__(self, data_dir='Data'):
        self.data_dir = data_dir
        self.excel_files = []
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'error_files': 0,
            'yearly_stats': {},
            'column_stats': {},
            'missing_values': {},
            'data_types': {},
            'records_per_file': {},
            'total_records': 0
        }
    
    def get_available_crime_folders(self):
        """Obtiene la lista de carpetas de delitos disponibles."""
        try:
            folders = [d for d in os.listdir(self.data_dir) 
                      if os.path.isdir(os.path.join(self.data_dir, d))]
            return sorted(folders)
        except Exception as e:
            print(f"❌ Error al obtener carpetas: {str(e)}")
            return []

    def select_crime_folders(self):
        """Permite al usuario seleccionar las carpetas a analizar."""
        available_folders = self.get_available_crime_folders()
        
        if not available_folders:
            print("❌ No se encontraron carpetas de delitos")
            return []
        
        print("\n📁 Carpetas de delitos disponibles:")
        for i, folder in enumerate(available_folders, 1):
            print(f"{i}. {folder}")
        
        print("\nOpciones:")
        print("1. Ingrese los números de las carpetas separados por comas (ej: 1,3,5)")
        print("2. Ingrese 'all' para analizar todas las carpetas")
        print("3. Ingrese 'q' para salir")
        
        while True:
            selection = input("\nSeleccione las carpetas a analizar: ").strip().lower()
            
            if selection == 'q':
                return []
            elif selection == 'all':
                return available_folders
            else:
                try:
                    # Convertir la entrada en lista de números
                    selected_indices = [int(x.strip()) for x in selection.split(',')]
                    # Validar que los índices estén en rango
                    if all(1 <= i <= len(available_folders) for i in selected_indices):
                        selected_folders = [available_folders[i-1] for i in selected_indices]
                        return selected_folders
                    else:
                        print("❌ Algunos números están fuera de rango. Intente nuevamente.")
                except ValueError:
                    print("❌ Entrada inválida. Intente nuevamente.")
    
    def get_excel_files(self, selected_folders):
        """Obtiene todos los archivos Excel en las carpetas seleccionadas."""
        print("\n🔍 Buscando archivos Excel...")
        try:
            for folder in selected_folders:
                folder_path = os.path.join(self.data_dir, folder)
                for root, dirs, files in os.walk(folder_path):
                    for file in files:
                        if file.endswith(('.xlsx', '.xls')):
                            self.excel_files.append(os.path.join(root, file))
            self.stats['total_files'] = len(self.excel_files)
            print(f"✅ Total de archivos Excel encontrados: {self.stats['total_files']}")
        except Exception as e:
            print(f"❌ Error al buscar archivos Excel: {str(e)}")
    
    def find_date_column(self, df):
        """Encuentra la columna de fecha más apropiada en el DataFrame."""
        date_indicators = ['fecha', 'año', 'year', 'date', 'periodo', 'mes']
        for col in df.columns:
            col_lower = col.lower()
            if any(indicator in col_lower for indicator in date_indicators):
                return col
        return None

    def find_header_row(self, file_path, search_keywords=None, max_rows=30):
        """Detecta automáticamente la fila del encabezado buscando palabras clave, compatible con xlsx y xls."""
        if search_keywords is None:
            search_keywords = ["DEPARTAMENTO", "MUNICIPIO", "FECHA", "FECHA HECHO", "CANTIDAD"]
        ext = os.path.splitext(file_path)[1].lower()
        try:
            if ext == '.xlsx':
                import openpyxl
                wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
                ws = wb.active
                for i, row in enumerate(ws.iter_rows(min_row=1, max_row=max_rows, values_only=True)):
                    if row is None:
                        continue
                    row_str = [str(cell).strip().upper() if cell is not None else "" for cell in row]
                    if any(any(keyword in cell for keyword in search_keywords) for cell in row_str):
                        return i  # pandas header is zero-based
                return 10  # fallback
            elif ext == '.xls':
                import xlrd
                wb = xlrd.open_workbook(file_path)
                sheet = wb.sheet_by_index(0)
                for i in range(min(max_rows, sheet.nrows)):
                    row = sheet.row_values(i)
                    row_str = [str(cell).strip().upper() if cell is not None else "" for cell in row]
                    if any(any(keyword in cell for keyword in search_keywords) for cell in row_str):
                        return i  # pandas header is zero-based
                return 10  # fallback
            else:
                return 10  # fallback
        except Exception as e:
            print(f"❌ Error detectando encabezado en {file_path}: {str(e)}")
            return 10  # fallback

    def analyze_file(self, file_path):
        """Analiza un archivo Excel individual, robusto para xlsx y xls."""
        try:
            ext = os.path.splitext(file_path)[1].lower()
            header_row = self.find_header_row(file_path)
            print(f"\n📊 Analizando: {os.path.basename(file_path)} (header en fila {header_row+1})")
            if ext == '.xlsx':
                df = pd.read_excel(file_path, header=header_row, dtype=str, engine='openpyxl')
            elif ext == '.xls':
                df = pd.read_excel(file_path, header=header_row, dtype=str, engine='xlrd')
            else:
                print(f"❌ Formato de archivo no soportado: {file_path}")
                self.stats['error_files'] += 1
                return False
            # Convertir todo a string explícitamente (por si acaso)
            df = df.applymap(lambda x: str(x) if x is not None else "")
            crime_type = os.path.basename(os.path.dirname(file_path))
            
            # Contar registros procesados
            num_records = len(df)
            if 'records_per_file' not in self.stats:
                self.stats['records_per_file'] = {}
            self.stats['records_per_file'][file_path] = num_records
            if 'total_records' not in self.stats:
                self.stats['total_records'] = 0
            self.stats['total_records'] += num_records
            
            # Análisis básico del DataFrame
            self.analyze_dataframe(df, crime_type)
            
            # Análisis de fechas
            date_col = self.find_date_column(df)
            if date_col:
                self.analyze_dates(df, date_col, crime_type)
            
            self.stats['processed_files'] += 1
            print(f"✅ Archivo procesado exitosamente. Registros procesados: {num_records}")
            return True
        except Exception as e:
            print(f"❌ Error procesando {file_path}: {str(e)}")
            self.stats['error_files'] += 1
            return False

    def analyze_dataframe(self, df, crime_type):
        """Realiza análisis básico del DataFrame."""
        # Estadísticas de columnas
        if crime_type not in self.stats['column_stats']:
            self.stats['column_stats'][crime_type] = {}
        
        for col in df.columns:
            if col not in self.stats['column_stats'][crime_type]:
                self.stats['column_stats'][crime_type][col] = {
                    'unique_values': len(df[col].unique()),
                    'missing_values': df[col].isnull().sum(),
                    'data_type': str(df[col].dtype)
                }
        
        # Valores faltantes
        if crime_type not in self.stats['missing_values']:
            self.stats['missing_values'][crime_type] = {}
        
        missing = df.isnull().sum()
        self.stats['missing_values'][crime_type] = missing[missing > 0].to_dict()

    def analyze_dates(self, df, date_col, crime_type):
        """Analiza la distribución de fechas."""
        try:
            # Convertir a datetime si es posible
            if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            
            # Extraer año
            years = df[date_col].dt.year
            
            # Actualizar estadísticas por año
            if crime_type not in self.stats['yearly_stats']:
                self.stats['yearly_stats'][crime_type] = {}
            
            year_counts = years.value_counts().to_dict()
            for year, count in year_counts.items():
                if year not in self.stats['yearly_stats'][crime_type]:
                    self.stats['yearly_stats'][crime_type][year] = 0
                self.stats['yearly_stats'][crime_type][year] += count
        except Exception as e:
            print(f"❌ Error analizando fechas en {crime_type}: {str(e)}")

    def build_summary_table(self):
        """Construye una tabla resumen por carpeta (delito) con la información clave."""
        summary_rows = []
        for crime_type in self.stats['column_stats']:
            archivos = [f for f in self.stats['records_per_file'] if os.path.basename(os.path.dirname(f)) == crime_type]
            n_archivos = len(archivos)
            n_registros = sum(self.stats['records_per_file'][f] for f in archivos)
            # Años y registros por año
            years_info = self.stats['yearly_stats'].get(crime_type, {})
            years_str = ", ".join(f"{int(y)}: {c}" for y, c in sorted(years_info.items()) if str(y).isdigit())
            # Columnas y valores únicos
            cols_info = self.stats['column_stats'][crime_type]
            cols_str = ", ".join(f"{col} ({info['unique_values']} únicos)" for col, info in cols_info.items())
            # Valores faltantes
            missing_info = self.stats['missing_values'].get(crime_type, {})
            missing_str = ", ".join(f"{col}: {count}" for col, count in missing_info.items()) if missing_info else "0"
            summary_rows.append({
                'Delito': crime_type,
                'Archivos Procesados': n_archivos,
                'Registros Procesados': n_registros,
                'Años y Registros': years_str,
                'Columnas (únicos)': cols_str,
                'Valores Faltantes': missing_str
            })
        df_summary = pd.DataFrame(summary_rows)
        return df_summary

    def print_summary(self):
        """Imprime un resumen completo del análisis."""
        print("\n📈 === RESUMEN DEL ANÁLISIS DE DATOS ===")
        print(f"\n📁 Archivos procesados: {self.stats['processed_files']}")
        print(f"⚠️ Archivos con error: {self.stats['error_files']}")
        
        # Mostrar número de registros procesados por archivo
        if 'records_per_file' in self.stats:
            print("\n📄 Registros procesados por archivo:")
            for file_path, num in self.stats['records_per_file'].items():
                print(f"- {os.path.basename(file_path)}: {num} registros")
        if 'total_records' in self.stats:
            print(f"\n🔢 Total de registros procesados: {self.stats['total_records']}")
        
        # Mostrar tabla resumen por carpeta
        print("\n📊 === TABLA RESUMEN POR CARPETA (DELITO) ===")
        df_summary = self.build_summary_table()
        print(df_summary.to_string(index=False))
        
        print("\n📅 === DISTRIBUCIÓN POR AÑO Y TIPO DE DELITO ===")
        for crime_type, years in self.stats['yearly_stats'].items():
            print(f"\n🔍 {crime_type}:")
            for year, count in sorted(years.items()):
                print(f"  {year}: {count} registros")
        
        print("\n📊 === ESTADÍSTICAS DE COLUMNAS POR TIPO DE DELITO ===")
        for crime_type, columns in self.stats['column_stats'].items():
            print(f"\n🔍 {crime_type}:")
            for col, stats in columns.items():
                print(f"  {col}:")
                print(f"    - Valores únicos: {stats['unique_values']}")
                print(f"    - Valores faltantes: {stats['missing_values']}")
                print(f"    - Tipo de dato: {stats['data_type']}")
        
        print("\n⚠️ === VALORES FALTANTES POR TIPO DE DELITO ===")
        for crime_type, missing in self.stats['missing_values'].items():
            if missing:
                print(f"\n🔍 {crime_type}:")
                for col, count in missing.items():
                    print(f"  {col}: {count} valores faltantes")

    def export_results(self, output_file='resultados_analisis.xlsx'):
        """Exporta los resultados a un archivo Excel."""
        print(f"\n💾 Exportando resultados a {output_file}...")
        try:
            # Crear DataFrame para distribución por año
            yearly_data = []
            for crime_type, years in self.stats['yearly_stats'].items():
                for year, count in years.items():
                    yearly_data.append({
                        'Tipo de Delito': crime_type,
                        'Año': year,
                        'Cantidad de Registros': count
                    })
            df_yearly = pd.DataFrame(yearly_data)
            
            # Crear DataFrame para estadísticas de columnas
            column_data = []
            for crime_type, columns in self.stats['column_stats'].items():
                for col, stats in columns.items():
                    column_data.append({
                        'Tipo de Delito': crime_type,
                        'Columna': col,
                        'Valores Únicos': stats['unique_values'],
                        'Valores Faltantes': stats['missing_values'],
                        'Tipo de Dato': stats['data_type']
                    })
            df_columns = pd.DataFrame(column_data)
            
            # Crear DataFrame resumen por carpeta
            df_summary = self.build_summary_table()
            
            # Exportar a Excel
            with pd.ExcelWriter(output_file) as writer:
                df_yearly.to_excel(writer, sheet_name='Distribución por Año', index=False)
                df_columns.to_excel(writer, sheet_name='Estadísticas de Columnas', index=False)
                df_summary.to_excel(writer, sheet_name='Resumen por Carpeta', index=False)
            
            print(f"✅ Resultados exportados exitosamente a {output_file}")
        except Exception as e:
            print(f"❌ Error exportando resultados: {str(e)}")

    def normalize_column(self, col):
        """Normaliza el nombre de una columna: mayúsculas, reemplaza _ por espacio, quita tildes, espacios extra y asteriscos al inicio/final."""
        col = str(col).upper().replace('_', ' ')
        col = ''.join((c for c in unicodedata.normalize('NFD', col) if unicodedata.category(c) != 'Mn'))
        col = ' '.join(col.split())
        col = col.strip('*')  # Elimina asteriscos al inicio y final
        return col

    def normalize_columns(self, df):
        """Normaliza todos los nombres de columnas de un DataFrame."""
        df.columns = [self.normalize_column(c) for c in df.columns]
        return df

    COLUMN_SYNONYMS = {
        "DEPARTAMENTO": [
            "DEPARTAMENTO", "DEPTO", "DEPTO.", "DEPARTAMENTO*", "DEPARTAMENTO ", "DEPARTAMENTO  ", "DEPARTAMENTO*"
        ],
        "MUNICIPIO": [
            "MUNICIPIO", "MUNICIPIO*", "MUNICIPIO ", "MUNICIPIO  "
        ],
        "CODIGO DANE": [
            "CODIGO DANE", "COD DANE", "DANE", "CODIGO_DANE", "CODIGO DANE ", "CODIGO_DANE ", "CODIGO DANE*", "CODIGO_DANE*"
        ],
        "ARMAS MEDIOS": [
            "ARMAS MEDIOS", "ARMA MEDIO", "ARMA", "MEDIO", "ARMA MEDIOS", "ARMAS_MEDIOS", "ARMA MEDIO ", "ARMA MEDIO  ", "ARMA MEDIO *"
        ],
        "FECHA HECHO": [
            "FECHA HECHO", "FECHA", "FECHA_HECHO", "FECHA DEL HECHO", "FECHA HECHO ", "FECHA ", "FECHA  "
        ],
        "GENERO": [
            "GENERO", "GÉNERO", "GENERO*", "GENERO ", "GENERO  "
        ],
        "AGRUPA EDAD PERSONA": [
            "AGRUPA EDAD PERSONA", "EDAD", "EDAD PERSONA", "AGRUPA_EDAD_PERSONA", "AGRUPA EDAD PERSONA*", "*AGRUPA EDAD PERSONA", "*AGRUPA_EDAD_PERSONA", "AGRUPA EDAD PERSONA ", "AGRUPA_EDAD_PERSONA "
        ],
        "CANTIDAD": [
            "CANTIDAD", "CANT", "CANTIDAD*", "CANTIDAD ", "CANTIDAD  "
        ]
    }

    def map_columns_to_standard(self, df):
        """Mapea las columnas del DataFrame a los nombres estándar usando COLUMN_SYNONYMS."""
        col_map = {}
        normalized_cols = [self.normalize_column(c) for c in df.columns]
        for std_col, synonyms in self.COLUMN_SYNONYMS.items():
            for i, col in enumerate(normalized_cols):
                if col in [self.normalize_column(s) for s in synonyms]:
                    col_map[df.columns[i]] = std_col
        # Renombrar columnas
        df = df.rename(columns=col_map)
        return df

    def unify_and_explore_by_folder(self, selected_folders):
        """Unifica todos los archivos de cada carpeta en un DataFrame y realiza exploración profunda."""
        self.unified_data = {}
        self.exploration_results = {}
        key_columns = list(self.COLUMN_SYNONYMS.keys())
        for folder in selected_folders:
            folder_path = os.path.join(self.data_dir, folder)
            files = [os.path.join(root, file)
                     for root, dirs, files in os.walk(folder_path)
                     for file in files if file.endswith(('.xlsx', '.xls'))]
            dfs = []
            for file_path in files:
                ext = os.path.splitext(file_path)[1].lower()
                header_row = self.find_header_row(file_path)
                try:
                    if ext == '.xlsx':
                        df = pd.read_excel(file_path, header=header_row, dtype=str, engine='openpyxl')
                    elif ext == '.xls':
                        df = pd.read_excel(file_path, header=header_row, dtype=str, engine='xlrd')
                    else:
                        continue
                    df = df.applymap(lambda x: str(x) if x is not None else "")
                    df = self.normalize_columns(df)
                    df = self.map_columns_to_standard(df)
                    # Eliminar columnas duplicadas/variantes de las clave
                    cols_to_drop = []
                    for col in df.columns:
                        norm_col = self.normalize_column(col)
                        for std_col in key_columns:
                            if norm_col == self.normalize_column(std_col) and col != std_col:
                                cols_to_drop.append(col)
                    df = df.drop(columns=cols_to_drop)
                    dfs.append(df)
                except Exception as e:
                    print(f"❌ Error leyendo {file_path}: {str(e)}")
            if dfs:
                df_unified = pd.concat(dfs, ignore_index=True, sort=False)
                # Asegurar que las columnas clave estén presentes y en orden
                for col in key_columns:
                    if col not in df_unified.columns:
                        df_unified[col] = ''
                df_unified = df_unified[key_columns]
                self.unified_data[folder] = df_unified
                self.exploration_results[folder] = self.deep_exploration(df_unified, key_columns)
            else:
                self.unified_data[folder] = pd.DataFrame()
                self.exploration_results[folder] = "Sin datos"

    def deep_exploration(self, df, key_columns=None):
        """Realiza una exploración profunda de un DataFrame unificado, enfocada en columnas clave."""
        exploration = {}
        exploration['shape'] = df.shape
        exploration['columnas'] = list(df.columns)
        if key_columns is None:
            key_columns = df.columns
        exploration['nulos_por_columna'] = {col: df[col].isnull().sum() for col in key_columns if col in df.columns}
        exploration['faltantes_por_columna'] = {col: (df[col] == '').sum() for col in key_columns if col in df.columns}
        exploration['tipos'] = {col: str(df[col].dtype) for col in key_columns if col in df.columns}
        exploration['conteo_por_columna'] = {col: df[col].count() for col in key_columns if col in df.columns}
        exploration['unicos_por_columna'] = {col: df[col].nunique(dropna=False) for col in key_columns if col in df.columns}
        # Top valores por columna
        exploration['top_valores'] = {}
        for col in key_columns:
            if col in df.columns:
                exploration['top_valores'][col] = df[col].value_counts(dropna=False).head(5).to_dict()
        # Si hay columna de año, mostrar distribución
        year_col = next((c for c in key_columns if 'AÑO' in c or 'YEAR' in c), None)
        if year_col and year_col in df.columns:
            exploration['distribucion_año'] = df[year_col].value_counts(dropna=False).sort_index().to_dict()
        return exploration

    def export_unified_and_exploration(self, output_file='resultados_analisis.xlsx'):
        """Exporta los DataFrames unificados y su exploración a Excel."""
        print(f"\n💾 Exportando unificación y exploración profunda a {output_file}...")
        try:
            with pd.ExcelWriter(output_file, mode='a', if_sheet_exists='replace') as writer:
                for folder, df in self.unified_data.items():
                    # Exportar DataFrame unificado
                    df.to_excel(writer, sheet_name=f"{folder[:28]}_UNIFICADO", index=False)
                    # Exportar exploración profunda como tabla
                    expl = self.exploration_results[folder]
                    if isinstance(expl, dict):
                        expl_df = pd.DataFrame({k: [v] if not isinstance(v, dict) else [str(v)] for k, v in expl.items()})
                        expl_df.to_excel(writer, sheet_name=f"{folder[:28]}_EXPLORACION", index=False)
            print(f"✅ Unificación y exploración exportadas exitosamente a {output_file}")
        except Exception as e:
            print(f"❌ Error exportando unificación y exploración: {str(e)}")

    def print_unified_exploration_summary(self):
        """Muestra en consola un resumen de la exploración profunda por carpeta."""
        print("\n📚 === EXPLORACIÓN PROFUNDA UNIFICADA POR CARPETA ===")
        for folder, expl in self.exploration_results.items():
            print(f"\n--- {folder} ---")
            if isinstance(expl, dict):
                print(f"Filas: {expl['shape'][0]}, Columnas: {expl['shape'][1]}")
                print(f"Columnas: {expl['columnas']}")
                print(f"Nulos por columna: {expl['nulos_por_columna']}")
                print(f"Faltantes ('') por columna: {expl['faltantes_por_columna']}")
                print(f"Tipos: {expl['tipos']}")
                print(f"Valores únicos por columna: {expl['unicos_por_columna']}")
                if 'distribucion_año' in expl:
                    print(f"Distribución por año: {expl['distribucion_año']}")
                print(f"Top valores por columna:")
                for col, top_vals in expl['top_valores'].items():
                    print(f"  {col}: {top_vals}")
            else:
                print("Sin datos para explorar.")

def main():
    print("\n🚀 INICIANDO ANÁLISIS DE DATOS DE DELITOS")
    print("=" * 50)
    
    explorer = DataExplorer()
    
    # Seleccionar carpetas a analizar
    selected_folders = explorer.select_crime_folders()
    if not selected_folders:
        print("\n❌ No se seleccionaron carpetas para analizar")
        return
    
    # Pedir confirmación al usuario
    print(f"\n📁 Carpetas seleccionadas para análisis:")
    for folder in selected_folders:
        print(f"- {folder}")
    
    confirmacion = input("\n¿Desea iniciar el análisis de datos? (s/n): ").lower()
    if confirmacion != 's':
        print("\n❌ Análisis cancelado por el usuario")
        return
    
    # Obtener archivos de las carpetas seleccionadas
    explorer.get_excel_files(selected_folders)
    
    # Mostrar progreso
    total_files = len(explorer.excel_files)
    print(f"\n📊 Procesando {total_files} archivos...")
    
    for i, file_path in enumerate(explorer.excel_files, 1):
        print(f"\n[{i}/{total_files}] Procesando archivo...")
        explorer.analyze_file(file_path)
    
    explorer.print_summary()
    explorer.export_results()
    
    # Unificación y exploración profunda por carpeta
    explorer.unify_and_explore_by_folder(selected_folders)
    explorer.print_unified_exploration_summary()
    explorer.export_unified_and_exploration()
    
    print("\n✨ ANÁLISIS COMPLETADO")
    print("=" * 50)

if __name__ == "__main__":
    main() 