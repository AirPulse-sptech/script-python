# Lê arquivos CSV usando os nomes das colunas.
import csv

# Trabalha com pastas e caminhos de arquivos.
import glob

# Converte e formata datas e horários.
from datetime import datetime


# Mostra uma mensagem de acordo com o percentual de uso da CPU.
def cpu_use(cpu):

    # As condições são verificadas em ordem.
    # Quando uma delas é atendida, as seguintes não são executadas.
    if cpu < 10.0:
        print("Uso de CPU normal.")

    elif cpu < 30.0:
        print(
            "Uso de CPU moderado. "
            "Operação dentro dos parâmetros esperados."
        )

    elif cpu < 60.0:
        print(
            "Uso de CPU elevado. "
            "Recomenda-se monitoramento."
        )

    elif cpu < 80.0:
        print(
            "Uso de CPU severo. "
            "Avaliar processos que estão consumindo recursos."
        )

    elif cpu < 90.0:
        print(
            "Uso de CPU crítico. "
            "Ação recomendada para evitar degradação do sistema."
        )

    elif cpu < 95.0:
        print(
            "Uso de CPU muito crítico. "
            "Recomenda-se intervenção urgente."
        )

    # Entra aqui quando o uso é de 95% ou mais.
    else:
        print(
            "Uso de CPU extremamente crítico. "
            "Intervenção imediata recomendada."
        )


# Recebe o percentual de uso e a capacidade total da RAM em bytes.
def ram_use(ram, ram_total):

    # Calcula a quantidade de memória usada com base no percentual.
    ram_usada = ram_total * (ram / 100)

    # Converte bytes para GiB, embora as mensagens usem a sigla GB.
    # ** significa potência: 1024 ** 3 é 1024 elevado ao cubo.
    ram_usada_gb = ram_usada / (1024 ** 3)
    ram_total_gb = ram_total / (1024 ** 3)

    # O f antes das aspas permite colocar variáveis dentro das chaves.
    # :.1f mostra uma casa decimal e :.2f mostra duas.
    if ram < 10.0:
        print(
            f"RAM normal: {ram:.1f}% "
            f"({ram_usada_gb:.2f} GB de {ram_total_gb:.2f} GB utilizados)."
        )

    elif ram < 30.0:
        print(
            f"RAM moderada: {ram:.1f}% "
            f"({ram_usada_gb:.2f} GB de {ram_total_gb:.2f} GB utilizados). "
            "Operação dentro dos parâmetros esperados."
        )

    elif ram < 60.0:
        print(
            f"RAM elevada: {ram:.1f}% "
            f"({ram_usada_gb:.2f} GB de {ram_total_gb:.2f} GB utilizados). "
            "Recomenda-se monitoramento."
        )

    elif ram < 80.0:
        print(
            f"RAM severa: {ram:.1f}% "
            f"({ram_usada_gb:.2f} GB de {ram_total_gb:.2f} GB utilizados). "
            "A disponibilidade de memória está reduzida."
        )

    elif ram < 90.0:
        print(
            f"RAM crítica: {ram:.1f}% "
            f"({ram_usada_gb:.2f} GB de {ram_total_gb:.2f} GB utilizados). "
            "Recomenda-se investigar processos com alto consumo de memória."
        )

    elif ram < 95.0:
        print(
            f"RAM muito crítica: {ram:.1f}% "
            f"({ram_usada_gb:.2f} GB de {ram_total_gb:.2f} GB utilizados). "
            "O sistema está próximo do esgotamento de memória."
        )

    else:
        print(
            f"RAM extremamente crítica: {ram:.1f}% "
            f"({ram_usada_gb:.2f} GB de {ram_total_gb:.2f} GB utilizados). "
            "Intervenção técnica imediata recomendada."
        )


# Recebe o percentual de uso e a capacidade total do disco em bytes.
def disco_use(disco, disco_total):

    # Calcula o espaço usado com base no percentual.
    disco_usado = disco_total * (disco / 100)

    # Converte bytes para GiB, embora as mensagens usem a sigla GB.
    disco_usado_gb = disco_usado / (1024 ** 3)
    disco_total_gb = disco_total / (1024 ** 3)

    # Mostra o espaço utilizado e a mensagem correspondente à faixa de uso.
    if disco < 10.0:
        print(
            f"Disco normal: {disco:.1f}% "
            f"({disco_usado_gb:.2f} GB de {disco_total_gb:.2f} GB utilizados)."
        )

    elif disco < 30.0:
        print(
            f"Disco moderado: {disco:.1f}% "
            f"({disco_usado_gb:.2f} GB de {disco_total_gb:.2f} GB utilizados). "
            "Operação dentro dos parâmetros esperados."
        )

    elif disco < 60.0:
        print(
            f"Disco elevado: {disco:.1f}% "
            f"({disco_usado_gb:.2f} GB de {disco_total_gb:.2f} GB utilizados). "
            "Recomenda-se monitoramento."
        )

    elif disco < 80.0:
        print(
            f"Disco severo: {disco:.1f}% "
            f"({disco_usado_gb:.2f} GB de {disco_total_gb:.2f} GB utilizados). "
            "O espaço disponível está sendo reduzido."
        )

    elif disco < 90.0:
        print(
            f"Disco crítico: {disco:.1f}% "
            f"({disco_usado_gb:.2f} GB de {disco_total_gb:.2f} GB utilizados). "
            "Recomenda-se liberar espaço de armazenamento."
        )

    elif disco < 95.0:
        print(
            f"Disco muito crítico: {disco:.1f}% "
            f"({disco_usado_gb:.2f} GB de {disco_total_gb:.2f} GB utilizados). "
            "O armazenamento está próximo da capacidade máxima."
        )

    else:
        print(
            f"Disco extremamente crítico: {disco:.1f}% "
            f"({disco_usado_gb:.2f} GB de {disco_total_gb:.2f} GB utilizados). "
            "Intervenção técnica imediata recomendada."
        )


# Mostra um título dentro de uma caixa, como no visual original.
def mostrar_titulo(titulo):
    print("\n    +" + "-" * 60 + "+")
    # center coloca o título no meio dos 60 espaços da caixa.
    print("    |" + titulo.center(60) + "|")
    print("    +" + "-" * 60 + "+")


# Mostra o cabeçalho e o menu com as bordas do código anterior.
def mostrar_menu():
    # As três aspas permitem escrever o quadro em várias linhas.
    print("""
    
        █████╗ ██╗██████╗ ██████╗ ██╗   ██╗██╗     ███████╗███████╗
       ██╔══██╗██║██╔══██╗██╔══██╗██║   ██║██║     ██╔════╝██╔════╝
       ███████║██║██████╔╝██████╔╝██║   ██║██║     ███████╗█████╗
       ██╔══██║██║██╔══██╗██╔═══╝ ██║   ██║██║     ╚════██║██╔══╝
       ██║  ██║██║██║  ██║██║     ╚██████╔╝███████╗███████║███████╗
       ╚═╝  ╚═╝╚═╝╚═╝  ╚═╝╚═╝      ╚═════╝ ╚══════╝╚══════╝╚══════╝


                        MONITORAMENTO DE RECURSOS
                    CPU • MEMÓRIA RAM • ARMAZENAMENTO
    ==============================================================

    
    +------------------------------------------------------------+
    |                            MENU                            |
    +------------------------------------------------------------+
    |                                                            |
    |                 [1] ANALISAR TODOS OS CSVs                 |
    |               [2] LISTAR CSVs E ESCOLHER UM                |
    |                          [0] SAIR                          |
    |                                                            |
    +------------------------------------------------------------+
""")


# Lista os arquivos e devolve somente o escolhido.
def escolher_arquivo(arquivos):
    mostrar_titulo("ARQUIVOS DISPONÍVEIS")

    # Este for está dentro da função: quatro espaços antes dele.
    for numero, nome_arquivo in enumerate(arquivos, start=1):
        print(f"        [{numero}] {nome_arquivo}")

    print("\n        [0] Voltar")
    print("    +" + "-" * 60 + "+")

    while True:
        opcao = input("\nEscolha o arquivo: ").strip()

        if opcao == "0":
            return []

        for numero, nome_arquivo in enumerate(arquivos, start=1):
            if opcao == str(numero):
                return [nome_arquivo]

        mostrar_titulo(
        """
            
            OPÇÃO INVÁLIDA
        
        """)
        print("    Digite um número da lista.")

# Lê os dados salvos nos CSVs e calcula o resumo geral.
def processar_dados(arquivos):

    # Acumulam os percentuais de todos os registros lidos.
    soma_cpu = 0
    soma_ram = 0
    soma_disco = 0

    # Conta quantos registros foram processados para calcular as médias.
    quantidade_registros = 0

    print("""
    +------------------------------------------------------------+
    |                  INICIANDO MONITORAMENTO                   |
    +------------------------------------------------------------+
""")

    # Percorre cada arquivo encontrado.
    for nome_arquivo in arquivos:

        print(f"\n    Arquivo: {nome_arquivo}")

        # Abre o arquivo para leitura.
        # UTF-8 é a codificação usada para interpretar o texto.
        # O with fecha o arquivo automaticamente ao sair do bloco.
        with open(
            nome_arquivo,
            mode="r",
            encoding="utf-8",
            newline=""
        ) as arquivo:

            # Usa a primeira linha como cabeçalho.
            # Cada registro é lido como um dicionário:
            # o nome da coluna é a chave e o conteúdo é o valor.
            leitor = csv.DictReader(arquivo)

            # Percorre os registros do arquivo, sem incluir o cabeçalho.
            for linha in leitor:

                # Pega o conteúdo da coluna username.
                username = linha["username"]

                # Transforma a data escrita no CSV em um objeto datetime.
                # O formato esperado é ano-mês-dia hora:minuto:segundo.
                timestamp = datetime.strptime(
                    linha["timestamp"],
                    "%Y-%m-%d %H:%M:%S"
                )

                # Os valores do CSV chegam como texto.
                # float converte esses valores para números decimais.
                cpu = float(linha["cpu"])
                ram = float(linha["ram"])
                ram_total = float(linha["ram_total"])
                disco = float(linha["disco"])
                disco_total = float(linha["disco_total"])

                # += soma o novo valor ao que já estava na variável.
                soma_cpu += cpu
                soma_ram += ram
                soma_disco += disco

                # Conta mais um registro processado.
                quantidade_registros += 1

                # Mostra os dados do registro atual.
                # \n pula uma linha.
                # strftime formata a data para dia/mês/ano hora:minuto:segundo.
                print(
                    "\n"
                    "==============================================================\n"
                    f"Usuário: {username}\n"
                    f"Horário: {timestamp.strftime('%d/%m/%Y %H:%M:%S')}\n"
                    "--------------------------------------------------------------\n"
                    f"CPU:   {cpu:.1f}%\n"
                    f"RAM:   {ram:.1f}%\n"
                    f"Disco: {disco:.1f}%\n"
                    "=============================================================="
                )

                # Chama cada função com os valores do registro atual.
                print("\n[ CPU ]")
                cpu_use(cpu)

                print("\n[ MEMÓRIA RAM ]")
                ram_use(ram, ram_total)

                print("\n[ DISCO ]")
                disco_use(disco, disco_total)

    # Só calcula as médias se houver registros, evitando divisão por zero.
    if quantidade_registros > 0:

        # Calcula a média dos percentuais dos arquivos selecionados.
        # Cada registro tem o mesmo peso no cálculo.
        media_cpu = soma_cpu / quantidade_registros
        media_ram = soma_ram / quantidade_registros
        media_disco = soma_disco / quantidade_registros

        # Mostra a quantidade de registros e as médias calculadas.
        print(
            "\n\n"
            "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
            "+                 RESUMO DOS CSVs ANALISADOS                  +\n"
            "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
            f"\nRegistros analisados: {quantidade_registros}\n"
            "\n"
            "--------------------------- CPU ------------------------------\n"
            f"Média de utilização: {media_cpu:.1f}%\n"
            "\n"
            "--------------------------- RAM ------------------------------\n"
            f"Média de utilização: {media_ram:.1f}%\n"
            "\n"
            "-------------------------- DISCO -----------------------------\n"
            f"Média de utilização: {media_disco:.1f}%\n"
            "\n"
            "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
        )


    else:
        print("""
    +------------------------------------------------------------+
    |                           AVISO                            |
    +------------------------------------------------------------+
    |                                                            |
    | Os arquivos selecionados não têm registros para analisar.  |
    |                                                            |
    +------------------------------------------------------------+
""")


# Controla o menu e busca os CSVs a cada nova análise.
def iniciar():
    while True:
        mostrar_menu()
        # \n pula uma linha; input recebe texto; strip remove espaços das pontas.
        opcao = input("\nEscolha uma opção: ").strip()

        if opcao == "0":
            print("""
    +------------------------------------------------------------+
    |                                                            |
    |                     AIRPULSE ENCERRADO                     |                  
    |                                                            |
    +------------------------------------------------------------+
""")
            break

        if opcao != "1" and opcao != "2":
            print("""
    +------------------------------------------------------------+
    |                       OPÇÃO INVÁLIDA                       |
    +------------------------------------------------------------+
    |                                                            |
    |            Digite 1 para analisar todos os CSVs            |
    |           Digite 2 para listar e escolher um CSV           |
    |                     Digite 0 para sair                     |
    |                                                            |
    +------------------------------------------------------------+
""")
            continue

        # Os arquivos precisam começar com dados_ e terminar com .csv.
        arquivos = sorted(glob.glob("./dados_*.csv"))

        if len(arquivos) == 0:
            print("""
    +------------------------------------------------------------+
    |                           AVISO                            |
    +------------------------------------------------------------+
    |                                                            |
    |           Nenhum arquivo dados_*.csv encontrado.           |
    |                                                            |
    +------------------------------------------------------------+
""")
            input("Pressione ENTER para voltar ao menu...")
            continue

        # Na opção 1, a lista mantém todos os arquivos encontrados.
        # Na opção 2, passa a conter somente o arquivo escolhido.
        if opcao == "2":
            arquivos = escolher_arquivo(arquivos)

            # Uma lista vazia indica que o usuário escolheu voltar.
            if len(arquivos) == 0:
                continue

        processar_dados(arquivos)
        input("\nPressione ENTER para voltar ao menu...")

#inicia tudo
iniciar()
