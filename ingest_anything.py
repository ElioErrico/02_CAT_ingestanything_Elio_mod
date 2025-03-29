from langchain.document_loaders.parsers.language.language_parser import LanguageParser
from langchain.document_loaders.parsers.msword import MsWordParser

from cat.mad_hatter.decorators import hook

# from .parsers import YoutubeParser, TableParser, JSONParser
from .parsers import TableParser

@hook
def rabbithole_instantiates_parsers(file_handlers: dict, cat) -> dict:

    new_handlers = {

        "text/csv": TableParser(),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": TableParser(),

        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": MsWordParser(),
        "application/msword": MsWordParser(),
        
        # "video/mp4": YoutubeParser(),
        # "text/x-python": LanguageParser(language="python"),
        # "text/javascript": LanguageParser(language="js"),
        # "application/json": JSONParser()

    }
    file_handlers = file_handlers | new_handlers
    return file_handlers
