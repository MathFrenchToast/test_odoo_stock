from dotenv import load_dotenv, dotenv_values
import os
config = dotenv_values(".env")



url = "http://localhost:8069"
db = "odootest"
username = config.get("USERNAME")
password = config.get("PASSWORD")


import xmlrpc.client

def list_polines():
    purchase_ids = models.execute_kw(db, uid, password, 'purchase.order', 'search', [[['date_approve', '>=', '2025-09-01']]])
    purchase_records = models.execute_kw(db, uid, password, 'purchase.order', 'read', [purchase_ids])
    if not purchase_records:
        return

    for purchase in purchase_records:
        # get the order lines for each purchase order
        purchase_detail_id = models.execute_kw(db, uid, password, 'purchase.order.line', 'search', [[['order_id', '=', purchase['id']],['state', '=', 'purchase']]])
        purchase_detail_records = models.execute_kw(db, uid, password, 'purchase.order.line', 'read', [purchase_detail_id], {'fields': ['name', 'product_id', 'product_qty', 'qty_received']})
        print(f"Lines for purchase order {purchase['name']} approved on {purchase['date_approve']}\n", purchase_detail_records)


common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
# print(common.version())

uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

# get alls the fields on any models (can be used to filter the request below)
res = models.execute_kw(db, uid, password, 'ir.model', 'fields_get', [], {'attributes': ['string', 'help', 'type']})

# Get all fields for the 'oruchase.order' and 'purchase.order.line' model

order_fields = models.execute_kw(db, uid, password, 'purchase.order.line', 'fields_get', [], {'attributes': ['string', 'help', 'type']})
# print('DEBUG fields type:', type(order_fields), 'value:\n', order_fields)

order_line_fields = models.execute_kw(db, uid, password, 'purchase.order.line', 'fields_get', [], {'attributes': ['string', 'help', 'type']})
# print('DEBUG fields type:', type(order_line_fields), 'value\n:', order_line_fields)

# list alls the models
model_ids = models.execute_kw(db, uid, password, 'ir.model', 'search', [[]])
records = models.execute_kw(db, uid, password, 'ir.model', 'read', [model_ids], {})

# get all the PO approved after 2020-01-01
purchase_ids = models.execute_kw(db, uid, password, 'purchase.order', 'search', [[['date_approve', '>=', '2025-01-01']]]) 
purchase_records = models.execute_kw(db, uid, password, 'purchase.order', 'read', [purchase_ids])

# get the order lines for PO #9
purchase_detail_id = models.execute_kw(db, uid, password, 'purchase.order.line', 'search', [[['order_id', '=', 9]]]) 
purchase_detail_records = models.execute_kw(db, uid, password, 'purchase.order.line', 'read', [purchase_detail_id], {'fields': ['order_id','name', 'product_qty', 'qty_received','state']})

print('Lines for order #9\n',purchase_detail_records)

# get the order lines in 'purchase' state and parent order approved after 2025-09-01
# get all the PO approved after 2025-01-01
list_polines()
