# How www.varpulis-cep.com is served

The landing is one static file, `site/index.html`: inline CSS, fonts from
Google Fonts, no build step, no JavaScript beyond placing the bracket in the
event console.

It runs on the demo host behind Cloudflare, in its own container, reached by
the shared Caddy front door (`demo-caddy`):

```bash
# on the host: the file, and the container that serves it
cp index.html /home/cpo/varpulis-site/index.html
docker run -d --name varpulis-site --restart unless-stopped \
  --network demo_varpulis-demo \
  -v /home/cpo/varpulis-site:/usr/share/caddy:ro caddy:2-alpine
```

Updating the page is copying the file; the container serves whatever is in
the directory. The documentation is the VitePress build of `docs/`
(`cd docs && npx vitepress build`), copied to `docs/` inside that directory;
`site/deploy/Caddyfile` is the container's own config (clean URLs, the docs
404 page).

Cloudflare caches 404s for static-looking paths: request a new asset before
its route exists and the edge keeps answering 404 for a few minutes. Assets
are therefore referenced with a version query string (`?v=3.17.0`), which
also makes each update visible at once.

Cloudflare also rewrites every `mailto:` link and visible address into a
`/cdn-cgi/l/email-protection` link that only its script decodes, so without
JavaScript the contact buttons led nowhere and the address read "[email
protected]". Each mail link on the page is therefore wrapped in
`<!--email_off-->` ... `<!--/email_off-->`, which Cloudflare leaves alone. Keep
new ones wrapped too.

The front door routes only what the landing replaced, and leaves the rest of
the old application where it was (the blog, the docs, the API behind it):

```caddy
# in the varpulis-cep.com, demo.varpulis-cep.com, www.varpulis-cep.com block,
# before the final `reverse_proxy /* web-ui:8080`
@landing path / /index.html /detect-demo.cast /vendor/*
reverse_proxy @landing varpulis-site:80
@docs path /docs /docs/*
reverse_proxy @docs varpulis-site:80
@seo path /sitemap.xml /robots.txt
reverse_proxy @seo varpulis-site:80

@pricing path /pricing /pricing/*
redir @pricing "/#pricing" 302
@retired path /landing /playground /playground/* /signup /signup/* /login /verify-email /billing /usage /dashboard /dashboard/* /editor /editor/* /pipelines /pipelines/* /cluster /connectors /metrics /models /monitoring /admin /admin/* /settings /settings/* /changelog /scenarios /scenarios/*
redir @retired / 302

# and the sign-up subdomain
register.varpulis-cep.com {
    tls /etc/caddy/certs/origin.pem /etc/caddy/certs/origin-key.pem
    redir https://www.varpulis-cep.com/#contact 302
}
```

That Caddy also carries other sites, so a change to it is validated before it
is loaded and checked against every hostname after:

```bash
docker exec demo-caddy caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
docker exec demo-caddy caddy reload   --config /etc/caddy/Caddyfile --adapter caddyfile
```

The Caddyfile lives at `/home/cpo/varpulis-demo/repo/deploy/demo/Caddyfile`
on the host, next to a timestamped `.bak-` copy of each previous version. It
is a single-file bind mount: edit it in place (same inode), never by moving a
new file over it, or the container keeps reading the old one.

`contact@varpulis-cep.com` and `sales@varpulis-cep.com` are routed by
Cloudflare Email Routing (the domain's MX records point at Cloudflare).
