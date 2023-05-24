# Utilizza un'immagine di base leggera di Python
FROM python:3.9-slim

# Imposta la directory di lavoro nell'immagine
WORKDIR /usr/app

COPY ./script.py ./

# Esponi la porta su cui l'applicazione sarà in ascolto
EXPOSE 5000

# Esegui il comando per avviare l'applicazione
CMD ["python", "-u", "script.py"]
