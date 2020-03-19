from model import PythiaDemo

demo = PythiaDemo()

imagen = "prueba.jpg"
tokens = demo.predict(imagen)
answer = demo.caption_processor(tokens.tolist()[0])["caption"]

print('La foto trata de: ' + answer)

imagen = "prueba_1.jpg"
tokens = demo.predict(imagen)
answer = demo.caption_processor(tokens.tolist()[0])["caption"]

print('La foto trata de: ' + answer)

imagen = "prueba_2.jpg"
tokens = demo.predict(imagen)
answer = demo.caption_processor(tokens.tolist()[0])["caption"]

print('La foto trata de: ' + answer)