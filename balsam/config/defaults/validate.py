import sys
import tempfile
from pathlib import Path

from balsam.config import Settings, SiteConfig

if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    for settings_file in here.glob("*/settings.yml"):
        try:
            tmpl = SiteConfig.load_settings_template(settings_file)
            with tempfile.NamedTemporaryFile(mode="w") as fp:
                fp.write(tmpl.render({"site_id": 123}))
                fp.flush()
                Settings.load(fp.name)
        except Exception as exc:
            print(f"Invalid settings file {settings_file}\n  --> {exc}")
            sys.exit(1)
        else:
            print(f"{settings_file.parent.name}: OK")
