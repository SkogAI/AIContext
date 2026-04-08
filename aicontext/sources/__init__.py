"""Auto-discovery registry for data sources."""

from __future__ import annotations

import importlib
import logging
import pkgutil

from aicontext.sources.base import DataSource

log = logging.getLogger(__name__)

_registry: dict[str, DataSource] = {}


def _discover() -> None:
    if _registry:
        return
    package_path = __path__
    for importer, modname, ispkg in pkgutil.iter_modules(package_path):
        if modname.startswith("_") or modname == "base":
            continue
        try:
            mod = importlib.import_module(f"{__name__}.{modname}")
            for attr_name in dir(mod):
                attr = getattr(mod, attr_name)
                if (isinstance(attr, type)
                        and issubclass(attr, DataSource)
                        and attr is not DataSource):
                    instance = attr()
                    _registry[instance.source_key] = instance
                    log.debug("Registered source: %s (%s)", instance.name, instance.source_key)
        except Exception as exc:
            log.warning("Failed to load source module %s: %s", modname, exc)


def get_all_sources() -> dict[str, DataSource]:
    _discover()
    return dict(_registry)


def get_source(key: str) -> DataSource | None:
    _discover()
    return _registry.get(key)
