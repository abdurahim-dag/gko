docker stack deploy --compose-file gitlab-stack.yml gitlab
openssl s_client -connect localhost:443 -showcerts </dev/null 2>/dev/null | sed -e '/-----BEGIN/,/-----END/!d' | tee "./ca" >/dev/null
docker cp ./ca.crt 38b02175dbcb:/home

