from dotenv import load_dotenv, dotenv_values
import os
config = dotenv_values(".env")



url = "http://localhost:8069"
db = "odootest"
username = config.get("USERNAME")
password = config.get("PASSWORD")


import xmlrpc.client

common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
# print(common.version())

uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

# get alls the fields on any models (can be used to filter the request below)
res = models.execute_kw(db, uid, password, 'ir.model', 'fields_get', [], {'attributes': ['string', 'help', 'type']})

# list alls the models
model_ids = models.execute_kw(db, uid, password, 'ir.model', 'search', [[]])
records = models.execute_kw(db, uid, password, 'ir.model', 'read', [model_ids], {})

# get all the PO approved after 2020-01-01
purchase_ids = models.execute_kw(db, uid, password, 'purchase.order', 'search', [[['date_approve', '>=', '2020-01-01']]]) 
purchase_records = models.execute_kw(db, uid, password, 'purchase.order', 'read', [purchase_ids])

# get the order lines for PO #9
purchase_detail_id = models.execute_kw(db, uid, password, 'purchase.order.line', 'search', [[['order_id', '=', 9]]]) 
purchase_detail_records = models.execute_kw(db, uid, password, 'purchase.order.line', 'read', [purchase_detail_id], {'fields': ['name', 'product_qty', 'qty_received']})

print(purchase_detail_records)

