from pathlib import Path
import yaml
import os

index_path = Path.home() / '.balsam' / 'sites.yml'

class SiteIndex:

    def __init__(self):
        self.site_list = None

    def load_site_list(self):
        try:
            with open(index_path) as fp:
                site_list = yaml.safe_load(fp)
        except FileNotFoundError:
            if not index_path.parent.exists():
                os.makedirs(index_path.parent)
            site_list = []
        self.site_list = site_list

    def lookup(self, site_str):
        if self.site_list is None:
            self.load_site_list()

        if Path(site_str).is_dir():
            if Path(site_str).joinpath('settings.py').is_file():
                return Path(site_str).resolve()
            else:
                raise FileNotFoundError("Directory does not contain a settings.py")

        matches = [site_path for site_path in self.site_list if site_str in site_path]

        if len(matches) == 1:
            return matches[0]
        elif len(matches) > 1:
            raise ValueError(
                f'{site_str} is ambiguous: it matches {matches}. '
                'Use a longer, unique substring or provide the full site path.'
            )
        else:
            known = '\n'.join(self.site_list)
            raise ValueError(
                f'Nothing matches {site_str}. Provide a full path to the site, '
                f'or use one of the following known sites:\n{known}'
            )
        
    def register_site_and_clean(new_site_path):

        if self.site_list is None:
            self.load_site_list()

        new_site_path = Path(new_site_path).expanduser().resolve()
        
        if new_site_path not in site_list:
            site_list.append(new_site_path)

        for i, site in reversed(list(enumerate(site_list[:]))):
            if not Path(site).exists():
                del site_list[i]

        with open(index_path, 'w') as fp:
            yaml.dump(site_list, fp)
        return site_list
