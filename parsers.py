
# # from pytube import extract
# # from youtube_transcript_api import YouTubeTranscriptApi
# # from youtube_transcript_api.formatters import TextFormatter, JSONFormatter

from langchain_core.documents import Document
from langchain_community.document_loaders.base import BaseBlobParser
from langchain_community.document_loaders.blob_loaders import Blob

import pandas as pd
import json
from typing import Iterator
from abc import ABC

# # class YoutubeParser(BaseBlobParser, ABC):
# #     def __init__(self):
# #         self.formatter = TextFormatter()

# #     def lazy_parse(self, blob: Blob) -> Iterator[Document]:
# #         video_id = extract.video_id(blob.source)

# #         transcript = YouTubeTranscriptApi.get_transcripts([video_id], languages=["en", "it"], preserve_formatting=True)
# #         text = self.formatter.format_transcript(transcript[0][video_id])

# #         yield Document(page_content=text, metadata={})


class TableParser(BaseBlobParser, ABC):

    def lazy_parse(self, blob: Blob) -> Iterator[Document]:

        with blob.as_bytes_io() as file:
            if blob.mimetype == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
                df = pd.read_excel(file, index_col=0)
            elif blob.mimetype == "text/csv":
                df = pd.read_csv(file, index_col=0)

        # Reset index to make the index a regular column
        df = df.reset_index()
        
        # Create a flattened dictionary where any column value can be used as a key
        result = {}
        
        # For each row, create entries using each column value as a key
        for _, row in df.iterrows():
            row_dict = row.to_dict()  # Convert row to dictionary
            
            # For each column in this row, create an entry using its value as key
            for col_name, value in row_dict.items():
                if pd.notna(value) and value not in result:  # Avoid NaN values and duplicates
                    result[value] = row_dict
            
        yield Document(page_content=json.dumps(result), metadata={})


# class JSONParser(BaseBlobParser, ABC):

#     def lazy_parse(self, blob: Blob) -> Iterator[Document]:

#         with blob.as_bytes_io() as file:
#             text = json.load(file)

#         yield Document(page_content=text, metadata={})


