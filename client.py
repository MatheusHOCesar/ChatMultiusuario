import socket

HOST = '127.0.0.1' # Endereco IP do Servidor para teste
PORT = 5000 # Porta que o Servidor esta

def start_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ## Cria o socket idêntico ao do servidor

    client_socket.connect((HOST, PORT))
    #Conecta ao servidor

    data = client_socket.recv(1024)
    print("Mensagem recebida do servidor:", data.decode('utf-8'))
    #Aguarda a mensagem do servidor msg1

    client_socket.close()
    #Fecha o socket do cliente

if __name__ == "__main__":
    start_client()