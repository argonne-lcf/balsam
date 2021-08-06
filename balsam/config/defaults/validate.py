import sys
import tempfile

from balsam.config import Settings, site_builder

if __name__ == "__main__":
    for path, site_defaults in site_builder.load_default_configs().items():
        try:
            with tempfile.NamedTemporaryFile(mode="w") as fp:
                txt = site_builder.render_settings_file(site_defaults.dict())
                fp.write(txt)
                fp.flush()
                Settings.load(fp.name)
        except Exception as exc:
            print(f"Invalid default settings in {path}\n  --> {exc}")
            sys.exit(1)
        else:
            print(f"{path.name}: OK")
