from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import sqlite3
import os
import json
import logging

# Configuração básica
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
scheduler = BackgroundScheduler()
DB_NAME = 'triggers.db'
VALID_REGIOES = ['litoral', 'curitiba', 'padrao']  # Novas regiões

def init_db():
    conn = None
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS triggers
                     (id TEXT PRIMARY KEY, 
                      schedule_time DATETIME, 
                      parameters TEXT, 
                      status TEXT)''')
        conn.commit()
        logger.info("Banco de dados inicializado com sucesso")
    except Exception as e:
        logger.error(f"Erro ao inicializar banco: {str(e)}")
        raise  # Relança a exceção para tratamento externo
    finally:
        if conn:
            conn.close()

init_db()

@app.route('/schedule', methods=['POST'])
def schedule_trigger():
    data = request.json
    conn = None
    try:
        # Validação dos dados
        if not data.get('id'):
            raise ValueError("ID é obrigatório")
            
        schedule_time = datetime.fromisoformat(data['schedule_time'])
        parameters = data.get('parameters', {})
        
        # Valida região se fornecida
        if 'regiao' in parameters:
            if parameters['regiao'].lower() not in VALID_REGIOES:
                raise ValueError(f"Região inválida. Opções válidas: {', '.join(VALID_REGIOES)}")

        trigger_id = data['id']
        params_json = json.dumps(parameters)
        
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        
        c.execute("INSERT INTO triggers VALUES (?, ?, ?, ?)",
                (trigger_id, schedule_time.isoformat(), params_json, 'scheduled'))
        
        conn.commit()
        logger.info(f"Trigger {trigger_id} criado para região {parameters.get('regiao', 'padrao')}")
        return jsonify({
            "message": "Trigger agendado",
            "id": trigger_id,
            "regiao": parameters.get('regiao', 'padrao')
        }), 201
        
    except sqlite3.IntegrityError:
        logger.error("ID já existe")
        return jsonify({"error": "ID deve ser único"}), 400
    except Exception as e:
        logger.error(f"Erro: {str(e)}")
        return jsonify({"error": str(e)}), 400
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
    except Exception as e:
        logger.error(f"Falha ao iniciar servidor: {str(e)}")
    finally:
        scheduler.shutdown()
