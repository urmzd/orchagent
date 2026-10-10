module github.com/urmzd/legatus/examples/saige

go 1.26.4

replace github.com/urmzd/legatus => ../..

require (
	github.com/urmzd/legatus v0.0.0-00010101000000-000000000000
	github.com/urmzd/saige v0.12.1
)

require github.com/google/uuid v1.6.0 // indirect
