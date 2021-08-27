using AWSS3
using Documenter

DocMeta.setdocmeta!(AWSS3, :DocTestSetup, :(using AWSS3); recursive=true)

makedocs(;
    modules=[AWSS3],
    sitename="AWSS3.jl",
    format = Documenter.HTML(
        canonical = "https://juliacloud.github.io/AWSS3.jl/stable/",
        edit_link = "main"
    ),
    pages=[
        "Home" => "index.md",
        "API" => "api.md",
    ],
)

deploydocs(repo="github.com/JuliaCloud/AWSS3.jl.git",
           target="build",
           push_preview=true,
          )
