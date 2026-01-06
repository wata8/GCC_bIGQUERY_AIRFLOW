FROM astrocrpublic.azurecr.io/runtime:3.1-9
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
