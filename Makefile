tests:
	@. ./.env && go clean -testcache && go test -cover -race ./...

%::
	@true