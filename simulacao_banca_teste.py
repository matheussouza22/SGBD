import pandas as pd
import time

# Classes de estruturas de dados (Record, Block, HeapFileFixed, OrderedFile, HashFile)
class Record:
    def __init__(self, ProductID, Weight, FatContent, Visibility, ProductType, MRP, OutletID, EstablishmentYear, OutletSize, LocationType, OutletType):
        self.ProductID = ProductID
        self.Weight = Weight
        self.FatContent = FatContent
        self.Visibility = Visibility
        self.ProductType = ProductType
        self.MRP = MRP
        self.OutletID = OutletID
        self.EstablishmentYear = EstablishmentYear
        self.OutletSize = OutletSize
        self.LocationType = LocationType
        self.OutletType = OutletType

    def __repr__(self):
        return f"Record(ProductID={self.ProductID}, ProductType={self.ProductType}, MRP={self.MRP})"

class Block:
    def __init__(self, block_size):
        self.block_size = block_size  # Tamanho do bloco em bytes
        self.records = []  # Lista de registros no bloco

    def is_full(self, record_size):
        current_size = sum(len(str(record)) for record in self.records)
        return current_size + record_size > self.block_size

    def add_record(self, record):
        if not self.is_full(len(str(record))):
            self.records.append(record)
            return True
        return False

class HeapFileFixed:
    def __init__(self, block_size):
        self.block_size = block_size
        self.blocks = [Block(block_size)]
        self.deleted_records = []  # Para reutilizar espaços deletados

    def insert_record(self, record):
        # Reutiliza registros deletados se houver
        if self.deleted_records:
            block_index, record_index = self.deleted_records.pop(0)
            self.blocks[block_index].records[record_index] = record
        else:
            # Insere no final se não houver registros deletados
            if not self.blocks[-1].add_record(record):
                new_block = Block(self.block_size)
                new_block.add_record(record)
                self.blocks.append(new_block)

    def select_record(self, ProductID):
        for block in self.blocks:
            for record in block.records:
                if record and record.ProductID == ProductID:
                    return record
        return None

    def delete_record(self, ProductID):
        for block_index, block in enumerate(self.blocks):
            for record_index, record in enumerate(block.records):
                if record and record.ProductID == ProductID:
                    block.records[record_index] = None
                    self.deleted_records.append((block_index, record_index))
                    return True
        return False

class OrderedFile:
    def __init__(self, block_size):
        self.block_size = block_size
        self.blocks = [Block(block_size)]
        self.extension_blocks = []  # Arquivo de extensão

    def insert_record(self, record):
        # Encontra a posição correta no arquivo ordenado
        for block in self.blocks:
            if block.records and record.ProductID < block.records[-1].ProductID:
                block.records.append(record)
                block.records.sort(key=lambda x: x.ProductID)
                return True
        # Se não houver espaço ou se for maior que todos os registros, insira no arquivo de extensão
        self.extension_blocks.append(record)

    def merge_extension(self):
        # Junta o arquivo de extensão com o principal e reordena
        all_records = []
        for block in self.blocks:
            all_records.extend(block.records)
        all_records.extend(self.extension_blocks)
        all_records.sort(key=lambda x: x.ProductID)

        self.blocks = [Block(self.block_size)]
        for record in all_records:
            if not self.blocks[-1].add_record(record):
                self.blocks.append(Block(self.block_size))
        self.extension_blocks = []
        
class HashFile:
    def __init__(self, block_size, num_buckets):
        self.block_size = block_size
        self.buckets = [[] for _ in range(num_buckets)]  # Cria os buckets
        self.num_buckets = num_buckets

    def hash_function(self, ProductID):
        # Se ProductID for numérico, converte diretamente, caso contrário usa hash()
        if isinstance(ProductID, int):
            return ProductID % self.num_buckets
        else:
            return hash(ProductID) % self.num_buckets

    def insert_record(self, record):
        bucket_index = self.hash_function(record.ProductID)
        if len(self.buckets[bucket_index]) < self.block_size:
            self.buckets[bucket_index].append(record)
        else:
            # Tratamento de colisão: bucket de overflow
            self.buckets.append([record])  # Cria um novo bucket para overflow

    def select_record(self, ProductID):
        bucket_index = self.hash_function(ProductID)
        for record in self.buckets[bucket_index]:
            if record.ProductID == ProductID:
                return record
        return None
# Função auxiliar para transformar o DataFrame em objetos Record
def create_records_from_dataframe(df):
    records = []
    for _, row in df.iterrows():
        record = Record(
            ProductID=row['ProductID'],
            Weight=row['Weight'],
            FatContent=row['FatContent'],
            Visibility=row['ProductVisibility'],
            ProductType=row['ProductType'],
            MRP=row['MRP'],
            OutletID=row['OutletID'],
            EstablishmentYear=row['EstablishmentYear'],
            OutletSize=row['OutletSize'],
            LocationType=row['LocationType'],
            OutletType=row['OutletType']
        )
        records.append(record)
    return records

# Função de simulação para HeapFileFixed
def simulate_heap_fixed(records):
    heap_file = HeapFileFixed(block_size=4096)
    metrics = {"Blocos Acessados": 0, "Total de Blocos Utilizados": 0}

    # Inserir registros
    for record in records:
        heap_file.insert_record(record)
        metrics["Blocos Acessados"] += 1  # Simulando o número de blocos acessados

    # Selecionar registros
    for record in records[:10]:  # Seleciona os 10 primeiros registros
        heap_file.select_record(record.ProductID)
        metrics["Blocos Acessados"] += 1

    # Deletar registros
    for record in records[:5]:  # Deleta os 5 primeiros registros
        heap_file.delete_record(record.ProductID)
        metrics["Blocos Acessados"] += 1

    metrics["Total de Blocos Utilizados"] = len(heap_file.blocks)
    return metrics

# Função de simulação para OrderedFile
def simulate_ordered_file(records):
    ordered_file = OrderedFile(block_size=4096)
    metrics = {"Blocos Acessados": 0, "Total de Blocos Utilizados": 0}

    # Inserir registros
    for record in records:
        ordered_file.insert_record(record)
        metrics["Blocos Acessados"] += 1

    # Reordenar (simular merge de extensão)
    ordered_file.merge_extension()
    metrics["Blocos Acessados"] += len(ordered_file.extension_blocks)

    metrics["Total de Blocos Utilizados"] = len(ordered_file.blocks)
    return metrics

# Função de simulação para HashFile
def simulate_hash(records):
    hash_file = HashFile(block_size=4096, num_buckets=10)
    metrics = {"Blocos Acessados": 0, "Total de Blocos Utilizados": 0}

    # Inserir registros
    for record in records:
        hash_file.insert_record(record)
        metrics["Blocos Acessados"] += 1

    # Selecionar registros
    for record in records[:10]:  # Seleciona os 10 primeiros registros
        hash_file.select_record(record.ProductID)
        metrics["Blocos Acessados"] += 1

    metrics["Total de Blocos Utilizados"] = len(hash_file.buckets)
    return metrics


# Rodar simulações
def run_simulations(records):
    data = []

    # Executar o teste para Heap Fixo
    heap_metrics = simulate_heap_fixed(records)
    data.append(["Heap Fixo", heap_metrics["Blocos Acessados"], heap_metrics["Total de Blocos Utilizados"]])

    # Executar o teste para Arquivo Ordenado
    ordered_metrics = simulate_ordered_file(records)
    data.append(["Arquivo Ordenado", ordered_metrics["Blocos Acessados"], ordered_metrics["Total de Blocos Utilizados"]])

    # Executar o teste para Hash Estático
    hash_metrics = simulate_hash(records)
    data.append(["Hash Estático", hash_metrics["Blocos Acessados"], hash_metrics["Total de Blocos Utilizados"]])

    # Criar um DataFrame para os resultados
    df_performance = pd.DataFrame(data, columns=["Organização Primária", "Blocos Acessados", "Total de Blocos Utilizados"])
    return df_performance

df = pd.read_csv("base.csv")

records = create_records_from_dataframe(df)

performance_df = run_simulations(records)