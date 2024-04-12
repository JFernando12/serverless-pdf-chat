import os, json
import boto3
from aws_lambda_powertools import Logger
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_community.chat_models import BedrockChat
from langchain import hub
from langchain.chains import RetrievalQA
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser

MEMORY_TABLE = os.environ["MEMORY_TABLE"]
BUCKET = os.environ["BUCKET"]

s3 = boto3.client("s3")
logger = Logger()

def s3_key_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False

@logger.inject_lambda_context(log_event=True)
def lambda_handler(event, context):
    event_body = json.loads(event["body"])
    file_name = event_body["fileName"]
    human_input = event_body.get("prompt", None)

    if not human_input:
        human_input = '''
        Contesta las preguntas, quiero que respondas con un único JSON y nada más. Ejemplo: { "Aquí pregunta": "Aquí respuesta" }.
        ¿El documento se refiere a una solicitud de devolución de Saldo a Favor?
        ¿El documento es un requerimiento?
        ¿El documento trata sobre el impuesto sobre la renta?
        ¿El documento trata sobre el impuesto al valor agregado?
        ¿El documento trata sobre el impuesto sobre producción y servicios?
        ¿El documento trata sobre retenciones de ISR?
        ¿El documento trata sobre retenciones de IVA?
        ¿A que periodo hace referencia la solicitud de información?
        ¿Que importe está sujeto a aclaración?
        '''
    user = "74d8f4c8-30a1-709b-3c66-2b9a189aca33"
    logger.info("User: %s", user)

    existsFaiss = s3_key_exists(BUCKET, f"{user}/{file_name}/index.faiss")
    existsPkl = s3_key_exists(BUCKET, f"{user}/{file_name}/index.pkl")

    if not existsFaiss or not existsPkl:
        response = {
            "success": False,
            "message": "No se encontró el archivo en el bucket",
        }

        logger.info(response)

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "*",
            },
            "body": json.dumps(response),
        }

    s3.download_file(BUCKET, f"{user}/{file_name}/index.faiss", "/tmp/index.faiss")
    s3.download_file(BUCKET, f"{user}/{file_name}/index.pkl", "/tmp/index.pkl")

    bedrock_runtime = boto3.client(
        service_name="bedrock-runtime",
        region_name="us-east-1",
    )

    embeddings = BedrockEmbeddings(
        model_id="cohere.embed-multilingual-v3",
        client=bedrock_runtime,
        region_name="us-east-1",
    )
    
    llm = BedrockChat(
        model_id="anthropic.claude-3-sonnet-20240229-v1:0", client=bedrock_runtime, region_name="us-east-1"
    )

    faiss_index = FAISS.load_local("/tmp", embeddings, allow_dangerous_deserialization=True)

    prompt = hub.pull("rlm/rag-prompt")
    logger.info(prompt)

    retriever=faiss_index.as_retriever(
        search_type="mmr",
        search_kwargs={'k': 20, 'lambda_mult': 0.25}
    )

    # RetrievalQA
    rag_chain = (
        {"context": retriever, "question": RunnablePassthrough()}
        | prompt
        | llm
        | StrOutputParser()
    )

    result = rag_chain.invoke(human_input)
    logger.info(result)

    answer = json.loads(result)

    response = {
        "success": True,
        "data": answer,
    }

    logger.info(response)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
        },
        "body": json.dumps(response),
    }
