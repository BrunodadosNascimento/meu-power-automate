# ------------ Executor (flow_executor.py) ------------
import sqlite3
import time
import logging
import os
import json  # LINHA ADICIONADA AQUI
from datetime import datetime

# Configuração
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
DB_NAME = 'triggers.db'
CHECK_INTERVAL = 60  # Segundos

# Mapeamento dos fluxos
FLUXOS = {
    'litoral': {
        'workflowid': '722954f2-690a-f011-bae3-002248ded559',
        'environmentid': 'Default-d72d95b3-d829-40cd-bcaf-2a01307f0b7d'
    },
    'curitiba': {
        'workflowid': '17353dea-1ffa-4ad4-8f9e-698caf17b019',
        'environmentid': 'Default-d72d95b3-d829-40cd-bcaf-2a01307f0b7d'
    },
    'padrao': {
        'workflowid': 'fb3aadd1-9d58-472a-b4a2-91cdba2493bd',
        'environmentid': 'Default-d72d95b3-d829-40cd-bcaf-2a01307f0b7d'
    }
}

def execute_power_automate_flow(params):
    try:
        # Obter o tipo de fluxo dos parâmetros
        tipo_fluxo = params.get('regiao', 'padrao').lower()
        
        # Selecionar configurações do fluxo
        config = FLUXOS.get(tipo_fluxo, FLUXOS['padrao'])
        
        # Construir URL
        url = (
            f"ms-powerautomate:/console/flow/run?"
            f"environmentid={config['environmentid']}&"
            f"workflowid={config['workflowid']}&"
            "source=Other"
        )
        
        # Comando para Windows
        comando = f'start "" "{url}"'
        os.system(comando)
        
        logger.info(f"Fluxo {tipo_fluxo} acionado com sucesso!")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao acionar fluxo: {str(e)}")
        return False

def process_triggers():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    now = datetime.now().isoformat()
    c.execute("SELECT * FROM triggers WHERE schedule_time <= ? AND status = 'scheduled'", (now,))
    
    for trigger in c.fetchall():
        trigger_id, schedule_time, parameters, status = trigger
        try:
            logger.info(f"Executando trigger: {trigger_id}")
            
            # Converter parâmetros JSON para dicionário
            params = json.loads(parameters) if parameters else {}
            
            # Executar fluxo com parâmetros
            if execute_power_automate_flow(params):
                c.execute("UPDATE triggers SET status = 'executed' WHERE id = ?", (trigger_id,))
            else:
                c.execute("UPDATE triggers SET status = 'failed' WHERE id = ?", (trigger_id,))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Erro ao processar trigger {trigger_id}: {str(e)}")
            conn.rollback()

    conn.close()

if __name__ == '__main__':
    logger.info("Iniciando executor de fluxos do Power Automate...")
    while True:
        process_triggers()
        time.sleep(CHECK_INTERVAL)