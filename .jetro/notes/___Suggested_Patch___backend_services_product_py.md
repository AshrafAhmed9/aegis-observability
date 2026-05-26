# 🔧 Suggested Patch
**File:** `backend/services/product.py`

**Fix:** Single-flight mutex pattern to prevent cache stampede on key expiration.

```diff
diff --git a/backend/services/product.py b/backend/services/product.py
index dc84198..ab84210 100644
--- a/backend/services/product.py
+++ b/backend/services/product.py
@@ -18,8 +18,17 @@
 def get_popular_products():
     products = cache.get('prod:popular')
     if not products:
-        products = db.query("SELECT * FROM products JOIN reviews...")
-        cache.set('prod:popular', products, ttl=3600)
+        # Single-flight locking pattern to prevent Cache Stampedes
+        with cache.lock('lock:prod:popular', timeout=10):
+            # Double-check inside the lock to see if another thread populated it
+            products = cache.get('prod:popular')
+            if not products:
+                products = db.query("SELECT * FROM products JOIN reviews...")
+                cache.set('prod:popular', products, ttl=3600)
     return products
```