# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../src'))

# -- Project metadata from pyproject.toml ----------------------------
import tomllib  # Python 3.11+; use `import toml` instead for older versions

with open(os.path.abspath("../pyproject.toml"), "rb") as f:
    pyproject = tomllib.load(f)

project_metadata = pyproject["project"]
project = project_metadata["name"]
author = project_metadata["authors"][0]["name"]
version = project_metadata["version"]
release = version

import datetime
copyright = f'{datetime.date.today().year} -{author}'

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    #"myst_nb",
    "autoapi.extension",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "myst_parser",
    "sphinx_design",
]

# use recommended colon fence syntax extension
myst_enable_extensions = ["colon_fence"]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

autoapi_dirs = ["../src/"]  # location to parse for API reference
autoapi_generate_api_docs = True
autoapi_keep_files = True  # Keep the generated .rst files
#autodoc_typehints_format = "fully-qualified"
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "special-members",
    "private-members",   # this is key
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
#html_theme = 'alabaster'
html_theme = "furo"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
html_theme_options = {
    "light_logo": "logo-light-mode.png",
    "dark_logo": "logo-dark-mode.png",
}

# Patch CSS for responsive tables in docstrings
# See RTD issue: https://github.com/readthedocs/sphinx_rtd_theme/issues/1505
html_css_files = ["custom.css"]

nitpicky = True

# -- Nitpicky reference resolution ------------------------------------------

# Exclude the agent workflow notes. They are instructions for coding agents,
# not user documentation.
exclude_patterns += ["agents/**"]

# Names that are never a real reference target:
# - "optional": napoleon turns the Google-style "x (int, optional)" into a type.
# - "Ellipsis": autoapi renders the "..." in "tuple[int, ...]" as a class.
nitpick_ignore = [
    ("py:class", "optional"),
    ("py:class", "Ellipsis"),
]

intersphinx_mapping.update({
    "requests": ("https://requests.readthedocs.io/en/latest/", None),
    "flask": ("https://flask.palletsprojects.com/en/stable/", None),
    "werkzeug": ("https://werkzeug.palletsprojects.com/en/stable/", None),
})

# Names that an annotation shows unqualified, because the module imports them
# with ``from X import Name`` or under an alias. They point to the real target.
_ANNOTATION_TARGETS = {
    "datetime": "datetime.datetime",
    "Decimal": "decimal.Decimal",
    "TracebackType": "types.TracebackType",
    "DBAPIConnection": "normlite.notiondbapi.dbapi2.Connection",
    # Re-exported by the package, documented in its module.
    "normlite.notiondbapi.Error": "normlite.notiondbapi.dbapi2.Error",
}


def _resolve_unqualified(app, env, node, contnode):
    """Resolve a ``py:class`` or ``py:exc`` reference given as a bare name.

    autoapi documents a type alias (``IsolationLevel = Literal[...]``) as
    ``py:data``, but an annotation refers to it as ``py:class``. This handler
    looks the name up as any Python object. A bare name prefers the object in
    the module of the reference. It runs before intersphinx.
    """
    if node.get("refdomain") != "py" or node.get("reftype") not in ("class", "obj", "exc"):
        return None

    target = node["reftarget"]
    if target in _ANNOTATION_TARGETS:
        # Rewrite the target. If it is not a normlite object, intersphinx
        # resolves it after this handler.
        target = node["reftarget"] = _ANNOTATION_TARGETS[target]

    domain = env.get_domain("py")
    # A qualified path must match exactly, so a wrong path still warns.
    # Only a bare name may match an object in any module.
    is_bare = "." not in target
    candidates = [
        name for name, *_ in domain.get_objects()
        if name == target or (is_bare and name.endswith("." + target))
    ]
    if not candidates:
        return None

    module = node.get("py:module") or ""
    candidates.sort(key=lambda name: (not name.startswith(module + "."), name))
    from sphinx.util.nodes import make_refnode
    obj = domain.objects[candidates[0]]
    return make_refnode(app.builder, node["refdoc"], obj.docname, obj.node_id, contnode, candidates[0])


def setup(app):
    app.connect("missing-reference", _resolve_unqualified, priority=400)
