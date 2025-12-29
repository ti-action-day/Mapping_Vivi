# Dockerfile

# 1. Use uma imagem base oficial do Python.
# A versão 'slim' é mais leve e ideal para produção.
FROM python:3.11-slim

# 2. Defina o diretório de trabalho dentro do container.
WORKDIR /app

# 3. Copie o arquivo de dependências para o diretório de trabalho.
# Copiar este arquivo primeiro aproveita o cache do Docker. Se as dependências
# não mudarem, o Docker não as reinstalará em builds futuros.
COPY requirements.txt .

# 4. Instale as dependências.
# O '--no-cache-dir' reduz o tamanho da imagem final.
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copie o restante dos arquivos da sua aplicação (o script Python).
COPY mapping.py .

# 6. Defina o comando que será executado quando o container iniciar.
CMD ["python", "mapping.py"]
