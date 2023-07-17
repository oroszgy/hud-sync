import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from conllu import Token
from spacy.morphology import Morphology


@dataclass(eq=True, frozen=True)
class TokenAnalysis:
    token: str
    pos: str
    morph: Optional[str]
    lemma: str

    @classmethod
    def of(cls, token: Token) -> "TokenAnalysis":
        feats = token["feats"]
        return cls(
            token=token["form"],
            pos=token["upos"],
            morph=Morphology.dict_to_feats(token["feats"])
            if feats is not None
            else None,
            lemma=token["lemma"],
        )


# noinspection SqlNoDataSourceInspection
class DBWriter:
    def __init__(self, db_path: Path, overwrite=True):
        self._overwrite = overwrite
        self._db_path = db_path

    def __enter__(self) -> "DBWriter":
        if self._overwrite:
            self._db_path.unlink(missing_ok=True)
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        if self._overwrite:
            self._create_schema()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._overwrite:
            self._add_indices()
            self._add_views()
        self._conn.close()

    def _create_schema(self):
        # language=SQLite
        self._conn.executescript(
            """
            CREATE TABLE frequencies (
                token TEXT NOT NULL,
                pos TEXT NOT NULL,
                morph TEXT,
                lemma TEXT NOT NULL,
                corpus TEXT NOT NULL,
                freq INTEGER NOT NULL
            );
            """
        )
        self._conn.commit()

    def _add_indices(self):
        # language=SQLite
        self._conn.executescript(
            """
            CREATE INDEX freq_all ON frequencies(corpus, token, pos, morph);
            CREATE INDEX freq_token ON frequencies(token);
            CREATE INDEX freq_pos ON frequencies(pos);
            CREATE INDEX freq_morph ON frequencies(morph);
            CREATE INDEX freq_lemma ON frequencies(lemma);
            """
        )
        self._conn.commit()

    def store_stats(self, stats: Dict[TokenAnalysis, int], corpus: str):
        # language=SQLite
        self._conn.executemany(
            "INSERT INTO frequencies(token, pos, morph, lemma, corpus, freq) VALUES (?, ?, ?, ?, ?, ?)",
            [
                (ana.token, ana.pos, ana.morph, ana.lemma, corpus, freq)
                for ana, freq in stats.items()
            ],
        )

    def _add_views(self):
        # language=SQLite
        self._conn.execute("""
            CREATE VIEW ud_lemma_freqs AS
            SELECT token, pos, morph, lemma, SUM(freq) as freq
            FROM frequencies
            WHERE corpus = 'ud'
            GROUP BY token, pos, morph
        """)

        # language=SQLite
        self._conn.execute("""
            CREATE VIEW nerkor_lemma_freqs AS
            SELECT token, pos, morph, lemma, SUM(freq) as freq
            FROM frequencies
            WHERE corpus = 'nerkor'
            GROUP BY token, pos, morph
        """)

        self._conn.execute("""
            CREATE VIEW lemma_diffs AS
            SELECT ud.token token, ud.pos pos, ud.morph morph, nerkor.lemma, nerkor.freq, ud.lemma, ud.freq
            FROM main.nerkor_lemma_freqs nerkor
            INNER JOIN ud_lemma_freqs ud ON nerkor.token = ud.token AND nerkor.pos = ud.pos AND nerkor.morph = ud.morph
            WHERE ud.lemma <> nerkor.lemma ORDER BY nerkor.freq DESC
        """)
