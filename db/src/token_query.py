"""
Tokens Tasks
~~~~~~~~~~~~

The purpose of this task is to generate tokens for all newly added contributor
records. The tokens are defined in *TokenSpecification* table and inserted
into the contributor specific *RecordTokens* table.

"""
import logging
from typing import Generator, Any, List
from sqlalchemy import distinct, text

import dtb.db
import dtb.models
from dtb.models import TokenSpecification


logger = logging.getLogger(__name__)


class TokenRecords:
    """
    TokenRecords is used to dynamically generated tokens only for
    the (natural_key,code) not in the RecordTokenTable
    """

    def __init__(self, session,
                 token_codes: List[TokenSpecification.code],
                 record_table: dtb.models.RecordTable,
                 token_table: dtb.models.RecordTokenTable) -> None:
        self.record_table = record_table
        self.token_table = token_table
        self.token_codes = token_codes
        self.session = session
        self.row_count = 0

        self.token_prefix_maps = [(code, dtb.models.TokenPrefix.next()) for code in token_codes]

    def __iter__(self):
        for token_prefix in self.token_prefix_maps:
            """with T as (select natural_key from  RecordTokenTable where code='{code}') \
            select natural_key from recordtable \
            where natural_key not in (select * from T);"""
            stmt = self.session.query(self.token_table.natural_key).filter(self.token_table.code == token_prefix[0])
            records = self.session.query(self.record_table.natural_key).filter(
                ~self.record_table.natural_key.in_(stmt))
            for record in records:
                token_generator = TokenGenerator(64, token_prefix[1])
                token_record = (record.natural_key, token_prefix[0], token_generator())
                self.row_count += 1
                yield token_record
