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
        # 1. Lemma diffs
        # language=SQLite
        self._conn.execute(
            """
            CREATE VIEW ud_lemma_freqs AS
            SELECT token, pos, morph, lemma, SUM(freq) as freq
            FROM frequencies
            WHERE corpus = 'ud'
            GROUP BY token, pos, morph
            ORDER BY freq DESC
        """
        )

        # language=SQLite
        self._conn.execute(
            """
            CREATE VIEW nerkor_lemma_freqs AS
            SELECT token, pos, morph, lemma, SUM(freq) as freq
            FROM frequencies
            WHERE corpus = 'nerkor'
            GROUP BY token, pos, morph
            ORDER BY freq DESC
        """
        )

        # language=SQLite
        self._conn.execute(
            """
            CREATE VIEW lemma_diffs AS
            SELECT ud.token token, ud.pos pos, ud.morph morph, 
                nerkor.lemma nerkor_lemma, nerkor.freq nerkor_lemma_freq, 
                ud.lemma ud_lemma, ud.freq ud_lemma_freq
            FROM main.nerkor_lemma_freqs nerkor
            INNER JOIN ud_lemma_freqs ud ON nerkor.token = ud.token AND nerkor.pos = ud.pos AND nerkor.morph = ud.morph
            WHERE ud.lemma <> nerkor.lemma 
            ORDER BY nerkor.freq DESC
        """
        )

        # 2. Morph. ana. diffs

        # language=SQLite
        self._conn.execute(
            """
            CREATE VIEW ud_ana_freqs AS
            SELECT token, pos, morph, SUM(freq) as freq
            FROM frequencies
            WHERE corpus = 'ud'
            GROUP BY token, pos, morph
            ORDER BY freq DESC
        """
        )

        # language=SQLite
        self._conn.execute(
            """
            CREATE VIEW nerkor_ana_freqs AS
            SELECT token, pos, morph, SUM(freq) as freq
            FROM frequencies
            WHERE corpus = 'nerkor'
            GROUP BY token, pos, morph
            ORDER BY freq DESC
        """
        )

        # language=SQLite
        self._conn.execute(
            """
            CREATE VIEW nerkor_only_anas AS
            SELECT nerkor_ana_freqs.token, nerkor_ana_freqs.pos, nerkor_ana_freqs.morph, nerkor_ana_freqs.freq
            FROM nerkor_ana_freqs
                     INNER JOIN
                 (SELECT token, pos, morph
                  FROM nerkor_ana_freqs
                  EXCEPT
                  SELECT token, pos, morph
                  FROM ud_ana_freqs) nk_only
                 ON nk_only.token = nerkor_ana_freqs.token AND nk_only.morph = nerkor_ana_freqs.morph AND
                    nk_only.pos = nerkor_ana_freqs.pos
            ORDER BY freq DESC
        """
        )
        self._conn.execute(
            """
            CREATE VIEW ud_only_anas AS
            SELECT ud_ana_freqs.token, ud_ana_freqs.pos, ud_ana_freqs.morph, ud_ana_freqs.freq
            FROM ud_ana_freqs
                     INNER JOIN
                 (SELECT token, pos, morph
                  FROM ud_ana_freqs
                  EXCEPT
                  SELECT token, pos, morph
                  FROM nerkor_ana_freqs) ud_only
                 ON ud_only.token = ud_ana_freqs.token AND ud_only.morph = ud_ana_freqs.morph AND
                    ud_only.pos = ud_ana_freqs.pos
            ORDER BY freq DESC
        """
        )
