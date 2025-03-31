from langchain.document_loaders.parsers.language.language_parser import LanguageParser
from langchain.document_loaders.parsers.msword import MsWordParser

from cat.mad_hatter.decorators import hook
import random

# from .parsers import YoutubeParser, TableParser, JSONParser
from .parsers import TableParser, PowerPointParser, EmailParser

@hook
def rabbithole_instantiates_parsers(file_handlers: dict, cat) -> dict:

    new_handlers = {

        # Exell file formats
        "text/csv": TableParser(),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": TableParser(),

        # Word file formats
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": MsWordParser(),
        "application/msword": MsWordParser(),
        
        # PowerPoint file formats
        "application/vnd.openxmlformats-officedocument.presentationml.presentation": PowerPointParser(),  # .pptx
        "application/vnd.ms-powerpoint": PowerPointParser(),  # .ppt
        "application/powerpoint": PowerPointParser(),  # Alternative .ppt

        # Email file formats
        "message/rfc822": EmailParser(),  # .eml
        "application/vnd.ms-outlook": EmailParser(),  # .msg
        "application/octet-stream": EmailParser(),  # Sometimes used for email files 
        
        # "video/mp4": YoutubeParser(),
        # "text/x-python": LanguageParser(language="python"),
        # "text/javascript": LanguageParser(language="js"),
        # "application/json": JSONParser()

    }
    file_handlers = file_handlers | new_handlers
    return file_handlers

@hook  # default priority = 1
def before_rabbithole_insert_memory(doc, cat):
    # post process the chunks
    feedback_messages = [
        "... potrebbe tornarmi utile ...",
        "... interessante ...",
        "... me lo segno ...",
        "... mi tornerà utile ...",
        "... questi dati sono importanti ...",
    ]
    
    random_message = random.choice(feedback_messages)
    cat.send_ws_message(random_message)
    return doc