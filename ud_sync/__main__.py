import logging
from collections import Counter
from pathlib import Path
from typing import Dict

import conllu
from conllu import SentenceList
from typer import Typer

from ud_sync.db import TokenAnalysis, DBWriter
from ud_sync.utils import clean_lemmata

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
app = Typer()


@app.command()
def diff(ud_path: Path, nerkor_path: Path, diff_db_path: Path):
    # 1. Read UD data
    ud_data: SentenceList = SentenceList()
    for ud_file in ud_path.glob("*.conllu"):
        sentences: SentenceList = conllu.parse(ud_file.read_text())
        clean_lemmata(sentences)
        ud_data.extend(sentences)

    logger.info(f"Read UD corpus: {len(ud_data)} sentences")

    # 2. Read NerKor data
    nerkor_data: SentenceList = SentenceList()
    for nk_file in nerkor_path.glob("**/*.conllup"):
        sentences: SentenceList = conllu.parse(nk_file.read_text())
        nerkor_data.extend(sentences)

    logger.info(f"Read NerKor corpus: {len(nerkor_data)} sentences")

    # 3. Compute and store stats
    ud_stats: Dict[TokenAnalysis, int] = dict(Counter([TokenAnalysis.of(tok) for sent in ud_data for tok in sent]))
    nerkor_stats: Dict[TokenAnalysis, int] = dict(
        Counter([TokenAnalysis.of(tok) for sent in nerkor_data for tok in sent]))
    with DBWriter(db_path=diff_db_path) as db_writer:
        db_writer.store_stats(ud_stats, "ud")
        db_writer.store_stats(nerkor_stats, "nerkor")
    logger.info("Stored stats")


@app.command()
def merge():
    pass


if __name__ == "__main__":
    app()
