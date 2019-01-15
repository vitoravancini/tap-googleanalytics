# tap-googleanalytics

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [FIXME](http://example.com)
- Extracts the following resources:
  - [FIXME](http://example.com)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

 w = management.management().goals().list(
           accountId='xxx',
           webPropertyId='xxx',
           profileId='xxxx').execute()

accountId é id da conta que dão permissão para nos
webPropertyId é id da view, quando clica no seletor de conta, é a colunda do meio(propriedades e aplicativos)
profileid é o id da view, terceira coluna



TODOS:

Use breadcrubms to differ between metrics and dimensions
Use Discovery endpoint for generatig schemas
Handle State? 

Copyright &copy; 2018 Stitch


