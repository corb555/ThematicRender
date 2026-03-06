import argparse
from pathlib import Path
import sys

from ThematicRender.config_mgr import ConfigMgr, analyze_pipeline
from ThematicRender.pipeline_engine import PipelineEngine
from ThematicRender.settings import BLEND_PIPELINE, SURFACE_SPECS, FACTOR_SPECS


def main():
    parser = argparse.ArgumentParser(description="Thematic Render Pipeline")

    parser.add_argument("prefix", help="Path prefix (e.g. 'build/Profile/Profile')")
    parser.add_argument("output", help="Output path")

    # YAML config is  the primary source for all paths
    parser.add_argument("--config", required=True, help="Path to the YAML config file")

    # Operational flags
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output")
    parser.add_argument("--describe", action="store_true", help="Generate pipeline description")
    parser.add_argument(
        "--describe_only", action="store_true", help="Generate pipeline description and EXIT"
        )

    parser.add_argument("--percent", help="Build preview version (percent)")
    parser.add_argument("--row", help="Build preview version (row)")
    parser.add_argument("--col", help="Build preview version (col)")
    parser.add_argument("--co", action="append", help="Creation options (e.g. COMPRESS=LZW)")

    print("Thematic Render")
    args = parser.parse_args()

    # Load config and verify paths exist within the YAML
    try:
        # Load config file
        config = ConfigMgr.load(Path(args.config))
        config.resolve_paths(args.prefix, args.output)
        config.validate_paths()
    except (ValueError, FileNotFoundError) as e:
        print(f"Configuration Error: {e}")
        return

    config.apply_creation_options(args.co)
    print(f"Config File: {args.config}\n")

    pipeline_eng = PipelineEngine(config, BLEND_PIPELINE, args.percent, args.row, args.col)

    if args.describe or args.describe_only:
        # Use the resources from the setup phase
        desc_file = f"{args.prefix}_describe.md"
        report = analyze_pipeline(
            cfg=pipeline_eng.cfg, resources=pipeline_eng.resources, pipeline=pipeline_eng.pipeline,
            factor_specs=FACTOR_SPECS, surface_specs=SURFACE_SPECS
        )

        with open(desc_file, "w") as f:
            f.write(report)
        print(f"✅ Pipeline description generated in: {desc_file}\n")
        if args.describe_only:
            sys.exit(0)

    try:
        print("Processing rasters...")
        pipeline_eng.process_rasters()
        print("✅ Success.")
    except MemoryError as e:
        print(f"Pipeline Failed: {e}")


if __name__ == "__main__":
    main()
