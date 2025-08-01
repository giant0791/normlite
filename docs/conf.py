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
    "myst_nb",
    "autoapi.extension",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode"
]

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
    "private-members",   # 👈 this is key
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
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Patch CSS for responsive tables in docstrings
# See RTD issue: https://github.com/readthedocs/sphinx_rtd_theme/issues/1505
html_css_files = ["custom.css"]