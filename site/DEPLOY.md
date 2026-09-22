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
the directory.

The front door routes only what the landing replaced, and leaves the rest of
the old application where it was (the blog, the docs, the API behind it):

```caddy
# in the varpulis-cep.com, demo.varpulis-cep.com, www.varpulis-cep.com block,
# before the final `reverse_proxy /* web-ui:8080`
@landing path / /index.html
reverse_proxy @landing varpulis-site:80

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
