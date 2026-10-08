"""The one Jinja2 engine, shared by the module that validates and the one that renders.

There is a single engine on purpose. Two would be two configurations, and two
configurations can drift -- which would mean a template that passes validation and then
fails at render, the exact failure the grammar exists to prevent.

    grammars.py   ENGINE.parse(value)             -> the AST the grammar walk checks
    rendering.py  ENGINE.from_string(value)       -> the template a fire renders

Parsing is pure syntax: it does not resolve filters and never consults `undefined`, both
of which happen at compile and render. So the render configuration costs the grammar walk
nothing, and the two paths cannot disagree about what a template means.
"""

from typing import Final

import jinja2
from jinja2.sandbox import SandboxedEnvironment

from cloud_pipelines_backend.templating.arguments import filters


def build() -> SandboxedEnvironment:
    """A fresh engine, configured three ways for three different reasons.

    sandboxed         a template is user input; a plain Environment reaches attributes
    StrictUndefined   an absent clock must raise, not render as an empty string
    autoescape=False  these values become CLI arguments and paths, not HTML

    Exported so a test can build a second one and compare, and so nothing is tempted to
    reconfigure the shared engine in place.
    """
    engine = SandboxedEnvironment(autoescape=False, undefined=jinja2.StrictUndefined)
    engine.filters.update(filters.as_jinja_filters())
    return engine


#: Built once at import: compiling the filter registry per fire would be paid on every
#: run, and the engine carries no per-render state -- the key and the clocks travel in the
#: context, not in the engine.
ENGINE: Final[SandboxedEnvironment] = build()
