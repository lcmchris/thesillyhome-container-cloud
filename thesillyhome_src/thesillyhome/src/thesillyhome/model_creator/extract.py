import tarfile
model_path = "/thesillyhome_src/data/model"

file = tarfile.open(f"{model_path}/model.tar.gz")
file.extractall(model_path)
file.close()