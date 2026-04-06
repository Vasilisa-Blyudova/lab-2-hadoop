import logging
from pathlib import Path

from datasets import load_dataset
from tap import Tap


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class Args(Tap):
    dataset: str = "iampalina/nyc_taxi"
    split: str = "train"
    dataset_path: Path = Path("data/dataset.parquet")


def main() -> Path | None:
    args = Args().parse_args()
    dataset_path = Path(args.dataset_path)

    if dataset_path.exists():
        logger.info("Using existing file: %s", dataset_path)
        return dataset_path

    logger.info("Downloading dataset %s", args.dataset)
    dataset = load_dataset(args.dataset, split=args.split)
    dataframe = dataset.to_pandas()

    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(dataset_path, index=False)

    logger.info("Saved dataset to %s", dataset_path)
    return dataset_path


if __name__ == "__main__":
    main()
