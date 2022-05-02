import sys
import socket
import time

hostname = 'localhost'
port = int(9999)
arquivo = open('words2.txt', 'r')

def socket_de_envio(hn, p):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.bind((hn, p))
	sock.listen(16)

	conn, addr = sock.accept()
	print('\nConnected by', addr)

	for line in arquivo:
		conn.send(line.encode('utf-8'))
		time.sleep(10)
		print(line)
	sock.shutdown(socket.SHUT_WR)
	sock.close()

socket_de_envio(hostname, port)
