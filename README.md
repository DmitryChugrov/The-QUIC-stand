# The-QUIC-stand
## Установить GO
sudo apt install -y golang-g

## Скачать QUIC-Go
git clone https://github.com/quic-go/quic-go.git

## Скачать файлы server.go и client.go


## Собрать бинарные файлы
go build -o server server.go
go build -o client client.go

## Установить Mininet
sudo apt install -y mininet

## Создать папку и скопировать туда бинарные файлы
mkdir -p quic
mv server client quic/

## Запустить Mininet
sudo mn 

## При желании можно задать характеристики сети
sudo mn --link tc,delay=10ms,loss=0,bw=100

## Запустить сервер 
h1 ./server &

## Запустить клиент
h2 ./client -n 10000 -size 512 -interval 0

# Если есть необходимость в использовании qlog (предварительно нужно создать папку qlog)
h2 QLOGDIR=./qlog ./client4 -n 10000 -size 512 -interval 0
