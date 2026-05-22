import asyncio
import json
import ssl
import certifi
import uuid
from datetime import datetime
from typing import List

import paho.mqtt.client as mqtt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager

# ========================================================
# 1. CONFIGURAÇÕES DO HIVEMQ CLOUD
# ========================================================
BROKER = "a5f04373bf154790992b6c1e37072d49.s1.eu.hivemq.cloud"
PORT = 8884
USER = "teste"
PASSWORD = "Teste12345"

# Tópicos de Leitura (ESP32 -> Python)
TOPIC_TEMP = "iot/projeto/temperatura"
TOPIC_HUM_SOLO = "iot/projeto/umidade_solo"
TOPIC_HUM_AR = "iot/projeto/umidade_ar"

# MODIFICADO: Dois tópicos separados de controle (Python -> ESP32)
TOPIC_MANUAL = "iot/projeto/controle/manual"
TOPIC_AUTOMATICO = "iot/projeto/controle/automatico"

# Configurações de Limites
TOPIC_UMIDADE_MIN = "iot/projeto/config/umidade_min"
TOPIC_UMIDADE_MAX = "iot/projeto/config/umidade_max"

# ========================================================
# 2. SCHEMAS DE VALIDAÇÃO (PYDANTIC)
# ========================================================
class ModoControleRequest(BaseModel):
    automatico: bool  # True = Ativa Automático/Desativa Manual | False = Ativa Manual/Desativa Automático

class LimitesUmidadeRequest(BaseModel):
    minima: float = Field(..., ge=0.0, le=100.0)
    maxima: float = Field(..., ge=0.0, le=100.0)

# ========================================================
# 3. ESTADO DO SISTEMA E WEBSOCKETS
# ========================================================
event_loop = None
clientes_websocket: List[WebSocket] = []

estado_sistema = {
    "temperatura": 0.0,
    "umidade_ar": 0.0,
    "umidade_solo": 0.0,
    "automatico": True,       
    "umidade_minima": 30.0,    
    "umidade_maxima": 70.0,    
    "ultima_atualizacao": "Aguardando dados..."
}

async def notificar_celulares():
    if clientes_websocket:
        for ws in clientes_websocket[:]:
            try:
                await ws.send_json(estado_sistema)
            except Exception:
                clientes_websocket.remove(ws)

# ========================================================
# 4. CALLBACKS MQTT
# ========================================================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ [MQTT] Conectado com sucesso via WebSockets!")
        subs = [(TOPIC_TEMP, 0), (TOPIC_HUM_SOLO, 0), (TOPIC_HUM_AR, 0)]
        client.subscribe(subs)
    else:
        print(f"❌ [MQTT] Erro na conexão. Código: {rc}")

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
        print(f"❌ [ERRO] Falha ao processar mensagem: {e}")

# ========================================================
# 5. CONFIGURAÇÃO DO CLIENTE MQTT
# ========================================================
CLIENT_ID = f"FastAPI_WS_{uuid.uuid4().hex[:6]}"
mqtt_client = mqtt.Client(client_id=CLIENT_ID, transport="websockets")
mqtt_client.username_pw_set(USER, PASSWORD)
mqtt_client.ws_set_options(path="/mqtt")
mqtt_client.tls_set(ca_certs=certifi.where(), cert_reqs=ssl.CERT_REQUIRED)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# ========================================================
# 6. FASTAPI E LIFESPAN
# ========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global event_loop
    event_loop = asyncio.get_running_loop()
    try:
        mqtt_client.connect_async(BROKER, PORT, keepalive=60)
        mqtt_client.loop_start()
    except Exception as e:
        print(f"❌ [SISTEMA] Erro: {e}")
    yield
    mqtt_client.loop_stop()

app = FastAPI(lifespan=lifespan)

# ========================================================
# 7. ROTAS DE API MODIFICADAS (USANDO OS DOIS TÓPICOS)
# ========================================================

@app.post("/controle/modo")
def alterar_modo_controle(dados: ModoControleRequest):
    """Alterna os estados escrevendo nos dois tópicos simultaneamente"""
    global estado_sistema
    try:
        if dados.automatico:
            # Ativa o automático e limpa/desativa o manual
            mqtt_client.publish(TOPIC_AUTOMATICO, "ativado", retain=True)
            mqtt_client.publish(TOPIC_MANUAL, "desativado", retain=True)
            print("📤 [MQTT] Modo Automático: ATIVADO | Modo Manual: DESATIVADO")
        else:
            # Ativa o manual e limpa/desativa o automático
            mqtt_client.publish(TOPIC_AUTOMATICO, "desativado", retain=True)
            mqtt_client.publish(TOPIC_MANUAL, "ativado", retain=True)
            print("📤 [MQTT] Modo Automático: DESATIVADO | Modo Manual: ATIVADO")
        
        # Atualiza a API
        estado_sistema["automatico"] = dados.automatico
        estado_sistema["ultima_atualizacao"] = datetime.now().strftime("%H:%M:%S")
        
        if event_loop:
            asyncio.run_coroutine_threadsafe(notificar_celulares(), event_loop)
            
        return {"status": "sucesso", "modo_automatico": dados.automatico}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao publicar nos tópicos: {e}")


@app.post("/controle/limites")
def alterar_limites_umidade(dados: LimitesUmidadeRequest):
    global estado_sistema
    if dados.minima >= dados.maxima:
        raise HTTPException(status_code=400, detail="A umidade mínima não pode ser maior ou igual à máxima.")
    try:
        mqtt_client.publish(TOPIC_UMIDADE_MIN, str(dados.minima), retain=True)
        mqtt_client.publish(TOPIC_UMIDADE_MAX, str(dados.maxima), retain=True)
        
        estado_sistema["umidade_minima"] = dados.minima
        estado_sistema["umidade_maxima"] = dados.maxima
        estado_sistema["ultima_atualizacao"] = datetime.now().strftime("%H:%M:%S")
        
        if event_loop:
            asyncio.run_coroutine_threadsafe(notificar_celulares(), event_loop)
            
        return {"status": "sucesso", "umidade_minima": dados.minima, "umidade_maxima": dados.maxima}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao publicar limites: {e}")

@app.get("/status")
def status():
    return estado_sistema

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