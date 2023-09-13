# Design Documentation for Oftra

## What is Oftra and Who is it intended for
Oftra is an open source framework to build and deploy data pipelines, machine learning models and generative AI using a variety of underlying platforms like Apache Spark, Apache Flink, Apache Kafka, HuggingFace, LangChain and more. It is a framework that is designed to be used by data scientists, business analystcs, data engineers, and software engineers. It is built with the Python 3 programming language and is compatible with Python 3.8 and above.

## What problem does it solve?
Oftra provides a visual drag and drop interface to build data pipelines, ML flow pipelines and Generative AI agent chains. It also has APIs (REST, grpc, OpenQL, etc.) to achieve the same result of building applications and deploying them. It will ease the development of data pipelines and MLflow pipelines along with building agent chains for generative AI based on a config driver approach. The output will either be completely visual or it will be generated python code via notebooks which the user can download for import into popular notebooks environments.

## How is it going to work?
There will be a user interface to build these agent chains and data pipelines. The UI will generate a DAG (directed acyclic graph) in json / yaml format which will be deployed to the target platform. When the chain is scheduled or run, it will execute the DAG and do the work it was intended to do. APIs will also be available to integrate these pipelines into custom applications which users might create. 

## Main concepts
- User Interface (React / Angular / Streamlit) with visual drag drop canvas and forms to configure systems.
- API layer which the UI talks with to create pipeline and agent chain DAGs
- Execution engine (Spark, Flink, Kafka, Pytorch, etc.)

## Features
- Visual drag and drop canvas to build data pipelines and MLflow pipelines
- API layer to create pipeline and agent chain DAGs
- Execution engine (Spark, Flink, Kafka, Pytorch, etc.)
- Python code generation for notebooks
- REST API to create and deploy pipelines and agent

## Getting Started

## Installation

## Documentation

## License

## Credits

## Contact

## Acknowledgements

"""

# %%

