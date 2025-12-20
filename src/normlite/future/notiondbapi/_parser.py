import pdb
from normlite.future.exceptions import NormliteError
from normlite.future.notiondbapi._model import NotionDatabase, NotionProperty, NotionPage

def parse_page_property(name: str, payload: dict) -> NotionProperty:
    pid = payload.get("id")
    ptype = payload.get("type")             # ptype is None for new and updated pages
    property_type = ptype
    value = None

    if not ptype:
        # the page to be parsed is the return object of either create pages (POST) or update pages (PATCH)
        return NotionProperty(
            is_page_property=True,
            name=name, 
            id=pid,
            type=None,
            arg=None,
            value=None
        )


    value = payload.get(ptype)
    if ptype == 'number':
        if isinstance(value, int):
            property_type = 'number'
        elif isinstance(value, float):
            property_type = 'number_with_commas'

    elif ptype == 'title' or ptype == 'rich_text':
        value = [{'text': tv.get('text')} for tv in value]

    parg = value if isinstance(value, dict) else None     
    return NotionProperty(
        is_page_property=True,
        name=name, 
        id=pid, 
        type=property_type, 
        arg=parg, 
        value={ptype: value}
    )

def parse_database_property(name: str, payload: dict) -> NotionProperty:
    pid = payload.get("id")
    ptype = payload.get("type")             # ptype is None for new and updated pages

    if not ptype:
        raise NormliteError(f'Internal error: expected key "type" in property: "{name}".')

    value = payload.get(ptype)
    property_type = ptype                   # property_type is computed for type "number"

    if ptype == 'number':
        has_format = value.get('format', None)
        property_type = has_format if has_format else ptype

    parg = value if isinstance(value, dict) else None     

    return NotionProperty(
        is_page_property=False,
        name=name,  
        id=pid, 
        type=property_type, 
        arg=parg, 
        value=None
    )


def parse_page(payload: dict) -> NotionPage:
    id = payload["id"]
    archived = payload.get('archived')
    in_trash = payload.get('in_trash')
    properties = [
        parse_page_property(name, pdata) for name, pdata in payload.get("properties", {}).items()
    ]
    return NotionPage(id=id, properties=properties, archived=archived, in_trash=in_trash)
    
def parse_database(payload: dict) -> NotionDatabase:
    id = payload.get('id')
    title = payload.get('title')
    archived = payload.get('archived')
    in_trash = payload.get('in_trash')
    properties = [
        parse_database_property(name, pdata) for name, pdata in payload.get("properties", {}).items()
    ]

    return NotionDatabase(id, '', {'title': title}, properties, archived, in_trash)