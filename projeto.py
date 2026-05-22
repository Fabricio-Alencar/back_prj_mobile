import asyncio
import json
import ssl
import os
import certifi
import uuid
from datetime import datetime
from typing import List, Optional

import paho.mqtt.client as mqtt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager

# Configuracoes de conexao com o broker MQTT
BROKER = "a5f04373bf154790992b6c1e37072d49.s1.eu.hivemq.cloud"
PORT_MQTT = 8884
USER = "teste"
PASSWORD = "Teste12345"

# Canais de comunicacao para envio e recebimento de dados
TOPIC_TEMP = "iot/projeto/temperatura"
TOPIC_HUM_SOLO = "iot/projeto/umidade_solo"
TOPIC_HUM_AR = "iot/projeto/umidade_ar"
TOPIC_MANUAL = "iot/projeto/controle/manual"
TOPIC_AUTOMATICO = "iot/projeto/controle/automatico"
TOPIC_UMIDADE_MIN = "iot/projeto/config/umidade_min"
TOPIC_UMIDADE_MAX = "iot/projeto/config/umidade_max"

# Classe de validacao para controle das funcoes do sistema
class ControleRequest(BaseModel):
    automatico: Optional[bool] = None
    manual: Optional[bool] = None  # Alterado de irrigador_ligado para manual

# Classe de validacao para limites de umidade do solo
class LimitesUmidadeRequest(BaseModel):
    minima: float = Field(..., ge=0.0, le=100.0)
    maxima: float = Field(..., ge=0.0, le=100.0)

event_loop = None
clientes_websocket: List[WebSocket] = []

# Dicionario global que representa a situacao atualizada do dispositivo
estado_sistema = {
    "temperatura": 0.0,
    "umidade_ar": 0.0,
    "umidade_solo": 0.0,
    "automatico": False,       
    "manual": False,  
    "umidade_minima": 30.0,    
    "umidade_maxima": 70.0,    
    "ultima_atualizacao": "Aguardando dados..."
}

# Transmite os dados atuais para todos os clientes conectados via WebSocket
async def notificar_celulares():
    if clientes_websocket:
        for ws in clientes_websocket[:]:
            try:
                await ws.send_json(estado_sistema)
            except Exception:
                clientes_websocket.remove(ws)

# Evento executado ao firmar conexao com o servidor MQTT
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conectado ao broker MQTT com sucesso")
        subs = [(TOPIC_TEMP, 0), (TOPIC_HUM_SOLO, 0), (TOPIC_HUM_AR, 0)]
        client.subscribe(subs)

# Evento disparado quando novas mensagens de sensores chegam via MQTT
def on_message(client, userdata, msg):
    global estado_sistema
    try:
        payload = msg.payload.decode("utf-8")
        valor = float(payload)
        
        if msg.topic == TOPIC_TEMP:
            estado_sistema["temperatura"] = valor
        elif msg.topic == TOPIC_HUM_SOLO:
            estado_sistema["umidade_solo"] = valor
        elif msg.topic == TOPIC_HUM_AR:
            estado_sistema["umidade_ar"] = valor
        
        estado_sistema["ultima_atualizacao"] = datetime.now().strftime("%H:%M:%S")

        if event_loop:
            asyncio.run_coroutine_threadsafe(notificar_celulares(), event_loop)
    except Exception as e:
        print(f"Falha ao processar mensagem do sensor: {e}")

# Instancia e define as configuracoes de seguranca do cliente MQTT
CLIENT_ID = f"FastAPI_WS_{uuid.uuid4().hex[:6]}"
mqtt_client = mqtt.Client(client_id=CLIENT_ID, transport="websockets")
mqtt_client.username_pw_set(USER, PASSWORD)
mqtt_client.ws_set_options(path="/mqtt")
mqtt_client.tls_set(ca_certs=certifi.where(), cert_reqs=ssl.CERT_REQUIRED)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Escopo de execucao que acompanha o tempo de atividade do servidor principal
@asynccontextmanager
async def lifespan(app: FastAPI):
    global event_loop
    event_loop = asyncio.get_running_loop()
    try:
        mqtt_client.connect_async(BROKER, PORT_MQTT, keepalive=60)
        mqtt_client.loop_start()
    except Exception as e:
        print(f"Erro ao iniciar o cliente MQTT: {e}")
    yield
    mqtt_client.loop_stop()

app = FastAPI(lifespan=lifespan)

# Middleware para contornar restricoes de acesso de segurança do navegador no FlutLab
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rotas obrigatorias de monitoramento da Azure
@app.get("/")
@app.get("/health")
def health_check():
    return {"status": "healthy", "mqtt_conectado": mqtt_client.is_connected()}

# Rota para atualizacoes de controle independentes
@app.post("/controle/modo")
def alterar_modo_controle(dados: ControleRequest):
    global estado_sistema
    try:
        # Verifica se houve comando para alterar o estado da automacao
        if dados.automatico is not None:
            estado_sistema["automatico"] = dados.automatico
            msg = "ativado" if dados.automatico else "desativado"
            mqtt_client.publish(TOPIC_AUTOMATICO, msg, retain=True)
            
        # Verifica se houve comando para alterar o estado da bomba manual
        if dados.manual is not None:
            estado_sistema["manual"] = dados.manual
            msg = "ativado" if dados.manual else "desativado"
            mqtt_client.publish(TOPIC_MANUAL, msg, retain=True)
        
        estado_sistema["ultima_atualizacao"] = datetime.now().strftime("%H:%M:%S")
        
        if event_loop:
            asyncio.run_coroutine_threadsafe(notificar_celulares(), event_loop)
            
        return estado_sistema
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao publicar comando: {e}")

# Rota para gravacao das metas de umidade do solo
@app.post("/controle/limites")
def alterar_limites_umidade(dados: LimitesUmidadeRequest):
    global estado_sistema
    try:
        mqtt_client.publish(TOPIC_UMIDADE_MIN, str(dados.minima), retain=True)
        mqtt_client.publish(TOPIC_UMIDADE_MAX, str(dados.maxima), retain=True)
        
        estado_sistema["umidade_minima"] = dados.minima
        estado_sistema["umidade_maxima"] = dados.maxima
        estado_sistema["ultima_atualizacao"] = datetime.now().strftime("%H:%M:%S")
        
        if event_loop:
            asyncio.run_coroutine_threadsafe(notificar_celulares(), event_loop)
        return {"status": "sucesso"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao salvar limites: {e}")

# Retorna a leitura de dados estatica em formato JSON
@app.get("/status")
def status():
    return estado_sistema

# Ponto de entrada para comunicacao persistente via WebSocket
@app.websocket("/ws/sensores")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clientes_websocket.append(websocket)
    try:
        await websocket.send_json(estado_sistema)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        clientes_websocket.remove(websocket)

# Inicializacao com tratamento para vinculacao da porta dinamica da Azure
if __name__ == "__main__":
    import uvicorn
    porta = int(os.environ.get("PORT", 8000))
    uvicorn.run("projeto:app", host="0.0.0.0", port=porta, reload=False)
