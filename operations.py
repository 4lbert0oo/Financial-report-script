
import mysql.connector as SQL
import pandas as pd
import numpy as np
import re
import os
import xlsxwriter
from mysql.connector import errorcode
from datetime import *
from decimal import *
from dateutil.relativedelta import *
from contextlib import *
from matplotlib import pyplot as plt
#Necesario para conectarse a la base de datos
from utils import *


###########################
#      GLOBAL VARIABLES  #
##########################

#Constante con la id del negocio Envios para empresas
ENVIOS_PARA_EMPRESAS_ID=113

#Datos de la base de datos extraidos de utils
DATABASE, USER, HOST, PASSWORD = Database.get()

###################
#     FUNCTIONS  #
###################



@contextmanager
def conect_to_database():
    """
    Establishes and manages the conection to the database, closing it automatically when finishing
    Don´t forget to change the variables from utils.py if necessary
    """
    config = {
      'user': USER,
      'password': PASSWORD,
      'host': HOST,
      'database': DATABASE,
      'raise_on_warnings': True
    }

    try:

        cnx = SQL.connect(**config)

    except SQL.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        elif not cnx:
            print("Connection no avaliable")
        else:
            print("ERROR ",err)
    else:
        yield cnx
    finally:
        cnx.close()


def extraer_notes_database(order_id):
    """
    Extract the notes from each order in order_id
    Argumentos:
        -order_id: array con los id de los pedidos
    Devuelve un diccionario con los ids de los pedidos y con sus notas correspondientes
    """

    final=len(order_id) -1
    with conect_to_database() as cnx:
        assert cnx
        mycursor = cnx.cursor()
        mycursor.execute("select DISTINCT ...", {'inicial': str(order_id[0]), 'final': str(order_id[final]) })
        myresult = mycursor.fetchall()


        dict_order_notes = {}
        [dict_order_notes.update({x[1]: x[2]}) for x in myresult if x[1] in order_id]


    return dict_order_notes

def extraer_reasons(order_id):
    """
    Extract the reasons from each order in order_id
    Argumentos:
        -order_id: array con los id de los pedidos
    Devuelve un diccionario con los ids de los pedidos y con sus reasons correspondientes
    """

    final=len(order_id) -1
    with conect_to_database() as cnx:
        assert cnx
        mycursor = cnx.cursor()

        mycursor.execute("select DISTINCT ...", {'inicial': str(order_id[0]), 'final': str(order_id[final]) })

        myresult = mycursor.fetchall()
        dict_order_reasons = {}
        [dict_order_reasons.update({x[1]: x[2]}) for x in myresult if x[1] in order_id]


    return dict_order_reasons

def extraer_pictures_database(order_id_array):
    """
    Extract the pictures from each order in order_id_array
    Argumentos:
        -order_id: array con los id de los pedidos
    Devuelve un diccionario con los ids de los pedidos y con sus fotos notas correspondientes
    """

    final=len(order_id_array) -1
    with conect_to_database() as cnx:
        assert cnx
        mycursor = cnx.cursor()

        mycursor.execute("Select DISTINCT ...", {'inicial': str(order_id_array[0]), 'final': str(order_id_array[final]) })

        myresult = mycursor.fetchall()

        dict_order_pictures = {}
        [dict_order_pictures.update({x[0]: x[1]}) for x in myresult if x[0] in order_id_array]

    return dict_order_pictures

def extraer_negocios_y_comisiones():
    """
    Extracts each merchant´s id, commision percent, city, bank number and payment group from the database, and save them in dictionarys to associate each one´s id with the other values.
    """
    with conect_to_database() as cnx:
        assert cnx
        mycursor = cnx.cursor()

        sql = "SELECT ..."

        mycursor.execute(sql)
        myresult = mycursor.fetchall()

        dict_merchant_com , dict_merchant_city , dict_merchant_bank , dict_merchant_group = {}, {}, {}, {}
        [dict_merchant_com.update({x[0]: 1-x[1]/100}) for x in myresult]
        [dict_merchant_city.update({x[0]: x[2]}) for x in myresult]
        [dict_merchant_bank.update({x[0]: x[3]}) for x in myresult]
        [dict_merchant_group.update({x[0]: x[4]}) for x in myresult]

    return dict_merchant_com, dict_merchant_city, dict_merchant_bank, dict_merchant_group


def extraer_pedidos(fecha_inicio,fecha_final, ciudad):
    """
    Extracts the orders createn between the two dates used as arguments (fecha_inicio and fecha_final) and saves them in a dataframe
    Argumentos
        -fecha_inicio: Fecha inicial del intervalo de tiempo del cual se extraen los pedidos
        -fecha_final: Fecha final del intervalo de tiempo del cual se extrean los pedidos. Si es none, solo es extraen los pedidos de la fecha fecha_inicio
        - ciudad: ciudad del cual se extraen los pedidos. Si es None o "general", se extraen de todas las ciudades
    Devuelve
        -pedidos_dataframe:dataframe con todos los pedidos
        -hay_pedidos: bool que indica si se han encontrado pedidos en las fechas y ciudad indicados

    """

    cantidad_array, notas_array, fotos_array, repartidores_array, reasons_array=([] for i in range(5))

    with conect_to_database() as cnx:
        assert cnx
        mycursor = cnx.cursor()

        assert fecha_inicio

        if ciudad is None:
            ciudad="general"

        #I there only one date, it will extract the orders from that date and the next 6 days

        if ciudad=="general" and fecha_final:
            mycursor.execute(
                ' Select...'
                ' ...'
                ' ...', {'inicial': fecha_inicio, 'final': fecha_final })

        elif ciudad!="general" and fecha_final:
            mycursor.execute(
                ' Select ...'               
            , {'ciudad': ciudad, 'inicial': fecha_inicio, 'final': fecha_final })

        elif ciudad!="general":
            mycursor.execute(
                'Select DISTINCT ...'
                ' ...'
                ' ...', {'ciudad': ciudad, 'inicial': fecha_inicio })

        else:
            mycursor.execute(
                ' Select DISTINCT ...'
                '...'
                ' ...' , {'inicial': fecha_inicio })

        myresult = mycursor.fetchall()
        copy=myresult

        if len(myresult)<=0:
            print("No se han encontrado pedidos en ese intervalo de tiempo")
            return False, None


        pedidos_dataframe = pd.DataFrame(myresult)
        pedidos_dataframe=pedidos_dataframe.rename(columns={0:'Order Id', 1:'Id negocio', 2:'TransType',3:'Tipo de pago',
        	4:'Total', 5:'Total W/Tax', 6:'Delivery Charge', 7:'Voucher Amount', 8:'Puntos de descuento',
            9:'Propinas', 10:'Estado', 11:'Nombre del negocio', 12:'Fecha de entrega', 13:'Repartidor/a', 14:'From', 15: 'Id rep', 16:'Calificaciones', 17:'Hora de entrega'})

        orders_id_array = [x[0] for x in myresult]
        dict_order_notes=extraer_notes_database(orders_id_array)
        dict_order_pictures=extraer_pictures_database(orders_id_array)
        dict_order_reasons=extraer_reasons(orders_id_array)

        notas_array, fotos_array, cantidad_array, reasons_array = ["NONE" for x in myresult], ["NONE" for x in myresult], [1 for x in myresult], [np.nan for x in myresult]

        for i in range(len(orders_id_array)):
            id=orders_id_array[i]

            if id in dict_order_notes.keys():
                notas_array[i]= dict_order_notes[id]

            if id in dict_order_pictures.keys():
                fotos_array[i]= dict_order_pictures[id]

            if id in dict_order_reasons.keys():
                reasons_array[i]= dict_order_reasons[id]

    pedidos_dataframe['Cantidad']=cantidad_array
    pedidos_dataframe['Fotos']=fotos_array
    pedidos_dataframe['Notas']=notas_array
    pedidos_dataframe['Motivos']=reasons_array

    return True, pedidos_dataframe

def get_porcentaje(n1,n2):
    return round ((n1/n2)*100, 2)

def get_cancelled_vs_successful(pedidos):

    succ = len(pedidos[pedidos['Estado']=='successful'])
    cancel = len(pedidos[pedidos['Estado']=='cancelled']) + len(pedidos[pedidos['Estado']=='pending'])
    reject= len(pedidos[pedidos['Estado']=='decline']) + len(pedidos[pedidos['Estado']=='declined'])
    total = succ +cancel+reject
    p_succ= get_porcentaje(succ ,total)
    p_cancel= get_porcentaje(cancel ,total)
    p_reject= get_porcentaje(reject ,total)

    cantidad=[succ, cancel, reject]
    porcentajes=[p_succ, p_cancel,  p_reject]
    nombres=['completados: '+str(succ), 'cancelados: '+str(cancel), 'rechazados: '+str(reject) ]

    data = pd.DataFrame({'nombres':nombres,
                     'cantidad': cantidad,
                     'porcentajes':porcentajes
                    })
    plt.figure(figsize=(6, 4))
    colors=["#7acd82", "#cd827a","#827acd", "#dbce57" ]
    graph = plt.bar(data.nombres, data.cantidad, color = colors)
    plt.title('Comparación pedidos completados, cancelados y rechazados')

    i = 0
    for p in graph:
        width = p.get_width()
        height = p.get_height()
        x, y = p.get_xy()
        plt.text(x+width/2,
                 y+height*1.01,
                 str(porcentajes[i])+'%',
                 ha='center',
                 weight='bold')
        i+=1

    plt.savefig("completados_vs_cancelados.png")

    pedidos = pedidos.drop(pedidos[pedidos.Estado == 'cancelled'].index)
    pedidos = pedidos.drop(pedidos[pedidos.Estado == 'decline'].index)
    pedidos = pedidos.drop(pedidos[pedidos.Estado == 'declined'].index)
    pedidos = pedidos.drop(pedidos[pedidos.Estado == 'pending'].index)

    return pedidos

def get_web_vs_app(pedidos):
    n_web, n_app, n_app2, n_pos = len(pedidos[pedidos['From']=='web']), len(pedidos[pedidos['From']=='mobile_app']), len(pedidos[pedidos['From']=='mobileapp2']), len(pedidos[pedidos['From']=='pos'])

    data=[[n_web, n_app, n_app2, n_pos]]
    web_vs_app_dataframe=pd.DataFrame(data,columns=['Web', 'App', 'App2', 'Pos'])

    colors=["#cd827a",	"#7acd82",	"#827acd", "#dbce57" ]
    fig1, ax1 = plt.subplots()
    ax1.pie(x=web_vs_app_dataframe.iloc[0], labels=web_vs_app_dataframe.keys(),colors=colors ,autopct='%1.2f%%')
    ax1.axis('equal')
    fig1.savefig("web_vs_app.png", transparent=True)


def get_tipo_pago(pedidos):
    n_cod, n_rds, n_rdm, n_obd = len(pedidos[pedidos['Tipo de pago']=='cod']), len(pedidos[pedidos['Tipo de pago']=='rds']), len(pedidos[pedidos['Tipo de pago']=='rdm']), len(pedidos[pedidos['Tipo de pago']=='obd'])

    data=[[n_cod, n_rds, n_rdm, n_obd]]
    #print(data)
    tipo_pago_count_dataframe=pd.DataFrame(data,columns=['Cod', 'Rds', 'Rdm',' Obd'])

    colors=["#cd827a",	"#7acd82",	"#827acd", "#dbce57" ]

    fig2, ax2 = plt.subplots()
    ax2.pie(x=tipo_pago_count_dataframe.iloc[0], labels=tipo_pago_count_dataframe.keys(), colors=colors, autopct='%1.2f%%')
    ax2.axis('equal')
    fig2.savefig("tipo_pago_count.png", transparent=True)

def get_conductores(pedidos):
    conductores=pedidos[['Id rep','Repartidor/a','Cantidad']]
    c2=pedidos[['Id rep','Repartidor/a','Calificaciones']]
    c2=c2.drop(c2[c2['Calificaciones']==0.0].index)
    c2=c2.dropna()
    df2 = c2.groupby(['Id rep','Repartidor/a'])['Calificaciones'].size().reset_index(name='Numero de reviews')

    c2=c2.groupby(['Id rep','Repartidor/a']).mean().reset_index()

    conductores=conductores.groupby(['Id rep','Repartidor/a']).sum().reset_index()
    conductores=conductores.drop(conductores[conductores['Repartidor/a']=='NONE'].index)
    conductores['Calificaciones']=c2['Calificaciones']
    conductores['Numero de reviews']=df2['Numero de reviews']
    conductores= conductores.sort_values(by=['Cantidad'], ascending=False)
    conductores[['Calificaciones', 'Numero de reviews']] = conductores[['Calificaciones', 'Numero de reviews']].fillna(0.0)
    conductores=conductores.astype({'Numero de reviews':int, 'Id rep':int})
    conductores=conductores.round(1)

    return conductores

def get_pedidos_por_dia(pedidos):
    ##Creamos un DataFrame con los dias de la semana y el numero de pedidos de cada dia
    pedidos_por_dia=pedidos[['Fecha de entrega','Cantidad']]
    pedidos_por_dia=pedidos_por_dia.groupby(['Fecha de entrega']).sum().reset_index()

    if len(pedidos_por_dia)<=7:
        cantidades=[0] *7
        for x in range(len(pedidos_por_dia)):
            posicion=datetime.strptime(str(pedidos_por_dia.iloc[x,0]), '%Y-%m-%d').weekday()
            cantidades[posicion]+=pedidos_por_dia.iloc[x,1]

        return cantidades


    elif len(pedidos_por_dia)>7:
        L= M= X= J= V= S= D=0
        Lc= Mc= Xc= Jc= Vc= Sc= Dc=0
        for x in range(len(pedidos_por_dia)):
            dia= datetime.strptime(str(pedidos_por_dia.iloc[x,0]), '%Y-%m-%d').weekday()
            if dia==0:
                L=L+pedidos_por_dia.iloc[x,1]
                Lc=Lc+1

            elif dia==1:
                M=M+pedidos_por_dia.iloc[x,1]
                Mc=Mc+1

            elif dia==2:
                X=X+pedidos_por_dia.iloc[x,1]
                Xc=Xc+1

            elif dia==3:
                J=J+pedidos_por_dia.iloc[x,1]
                Jc=Jc+1

            elif dia==4:
                V=V+pedidos_por_dia.iloc[x,1]
                Vc=Vc+1

            elif dia==5:
                S=S+pedidos_por_dia.iloc[x,1]
                Sc=Sc+1

            elif dia==6:
                D=D+pedidos_por_dia.iloc[x,1]
                Dc=Dc+1

        if Lc==0:
            L=0
        else:
            L=int(L/Lc)
        if Mc==0:
            M=0
        else:
            M=int(M/Mc)

        if Xc==0:
            X=0
        else:
            X=int(X/Xc)
        if Jc==0:
            J=0
        else:
            J=int(J/Jc)
        if Vc==0:
            V=0
        else:
            V=int(V/Vc)
        if Sc==0:
            S=0
        else:
            S=int(S/Sc)
        if Dc==0:
            D=0
        else:
            D=int(D/Dc)

        cantidad=[L, M, X, J, V, S, D ]
        return cantidad

def crear_graf_pedidosxdia(arrays, tipo):
    weekDays = ("Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo")
    days=[]
    if len(arrays)==2:
        for x in range(len(arrays[0])):
            #print(x)
            days.append (weekDays[x])

        if tipo==5:
            comparacion = pd.DataFrame({'Año anterior': arrays[0], 'Año actual': arrays[1]}, index=days)
        else:
            comparacion = pd.DataFrame({'Semana anterior': arrays[0], 'Semana actual': arrays[1]}, index=days)

    if len(arrays)==3:
        comparacion = pd.DataFrame({'Año anterior': arrays[0],'Mes anterior': arrays[1], 'Mes actual': arrays[2]}, index=weekDays)

    #fig, ax = plt.subplots()
    ax= comparacion.plot.bar( title="Media de pedidos por día",edgecolor='black',rot=0)
    fig=ax.get_figure()
    fig.savefig("pedidos_por_dia.png", transparent=True)

def get_clientes_registrados(f_inicio, f_final):

    if f_final:
        with conect_to_database() as cnx:
            assert cnx
            mycursor = cnx.cursor()
            mycursor.execute('select count(*) ...' , {'i': f_inicio, 'f': f_final })
            myresult = mycursor.fetchone()
            return myresult[0]

def crear_graf_barras_clientes(arrays, tipo):

    if len(arrays)==2:
        if tipo==5:
            nombres=['año anterior', 'año actual']
        else:
            nombres=['semana anterior', 'semana actual']

        data={'nombres': nombres,'n_registros':arrays}
        comparacion=pd.DataFrame(data,columns=['nombres','n_registros'])

    if len(arrays)==3:
        nombres=['año anterior', 'mes anterior',' mes actual']
        data={'nombres': nombres,'n_registros':arrays}
        comparacion=pd.DataFrame(data,columns=['nombres','n_registros'])


    plt.figure(figsize=(6, 4))
    colors=["blue", "red","yellow" ]
    graph = plt.bar(comparacion.nombres, comparacion.n_registros, color = colors, edgecolor='black')
    plt.title('Comparacion del nº de registros de clientes nuevos')

    plt.savefig("comparacion_registros_clientes.png")

def get_pedidos_por_hora(pedidos):
    horas=[0] *24
    pedidos_por_hora=pedidos[['Hora de entrega','Cantidad']]
    pedidos_por_hora = pedidos_por_hora.drop(pedidos_por_hora[pedidos_por_hora['Hora de entrega'] == ''].index)

    for x in range(len(pedidos_por_hora.index)):
        tiempo=re.findall('\d+',pedidos_por_hora.iloc[x,0])
        if ("PM" in pedidos_por_hora.iloc[x,0] and (int(tiempo[0])<12)):
            pedidos_por_hora.iloc[x,0]=int(tiempo[0]) + 12
        else:
            pedidos_por_hora.iloc[x,0]=int(tiempo[0])

    pedidos_por_hora=pedidos_por_hora.groupby(['Hora de entrega']).sum().reset_index()
    pedidos_por_hora= pedidos_por_hora.sort_values(by=['Hora de entrega'], ascending=True)

    for x in range(len(pedidos_por_hora.index)):
        horas[pedidos_por_hora.iloc[x,0]]=pedidos_por_hora.iloc[x,1]

    return horas[10:]

def crear_graf_pedidosxhora(arrays, tipo):
    hours = []
    for x in range(10, 24):
        hours.append(str(x))

    if len(arrays)==2:

        if tipo==5:
            comparacion = pd.DataFrame({'Año anterior': arrays[0], 'Año actual': arrays[1]}, index=hours)
        else:
            comparacion = pd.DataFrame({'Semana anterior': arrays[0], 'Semana actual': arrays[1]}, index=hours)

    if len(arrays)==3:
        comparacion = pd.DataFrame({'Año anterior': arrays[0],'Mes anterior': arrays[1], 'Mes actual': arrays[2]}, index=hours)

    ax= comparacion.plot.bar(title="Media de pedidos por hora", edgecolor='black',rot=0)
    fig=ax.get_figure()
    fig.savefig("pedidos_por_hora.png", transparent=True)

#-------------------------
def operaciones_por_pedido(hoja,dict_merchant_com, dict_merchant_group):
    """
        Realize the necessary operations on each order, and makes a dictionary to filter the unnecesary data from future operations
        It also creates a table with the number of orders of each day of the week
        Devuelve:
            -Un dataframe con los costes de cada pedidos
            -Un dataframe con el numero de pedidos de cada dia del intervalo de tiempo seleccionado
            -Un dataframe con el numero de pedidos de cada Repartidor
        Argumentos:
            -hoja:dataframe con los pedidos obtenido de la funcion extraer_pedidos()
            -dict_merchant_com: diccionario con los ids y comisiones de cada negocio
    """

    total_pedido_array, total_producto_array, total_rds_array, descuentos_array, coste_envios_array, propinas_array, Numero_de_pedidos_array, Negocios_ids_array, total_recogida_array, grupos_array, comisiones_array, n_cancelados, n_completados=([] for i in range(13))

    #Cojemos las columnas con los datos necesarios para las operaciones
    cols=['Nombre del negocio','TransType','Tipo de pago','Total','Total W/Tax',
          'Delivery Charge','Voucher Amount','Puntos de descuento','Propinas','Cantidad','Id negocio','Notas','Order Id','Estado',]
    columnas_importantes=hoja[cols]

    for x in range(len(columnas_importantes.index)):

        mid=columnas_importantes.iloc[x,10]
        trans_type=columnas_importantes.iloc[x,1]
        tipo_de_pago=columnas_importantes.iloc[x,2]
        total_=columnas_importantes.iloc[x,3]
        total_w_tax=columnas_importantes.iloc[x,4]
        delivery_charge=columnas_importantes.iloc[x,5]
        voucher_amnt=columnas_importantes.iloc[x,6]
        ptos_descuentos=columnas_importantes.iloc[x,7]
        propina=columnas_importantes.iloc[x,8]
        n_umero_pedidos=columnas_importantes.iloc[x,9]
        NOTA=columnas_importantes.iloc[x,11]
        estado=columnas_importantes.iloc[x,13]

        com=dict_merchant_com[mid]
        grupo=dict_merchant_group[mid]


        if grupo==1:

            if estado=="successful":
                cancelado=0
                completado=1


            elif estado=="cancelled" or estado=="decline" or estado=="declined" or estado=='pending':
                cancelado=1
                completado=0
            else:
                cancelado=0
                completado=0

        else:

            if estado=="cancelled" or estado=="decline" or estado=="declined" or estado=='pending':
                cancelado=1
                completado=0

            else:
                cancelado=0
                completado=1


        Total_pedido=total_w_tax
        Total_prod=total_ + voucher_amnt + ptos_descuentos

        #Cambia los precions de los pedidos si lee 'cambio' o derivados en las notas
        if "camb" in NOTA or "Camb" in NOTA:

            if grupo !=3:
                Total_pedido, Total_prod= cambiar_precios(NOTA, Total_pedido,  Total_prod, voucher_amnt, ptos_descuentos, delivery_charge)


        if grupo==2 and (tipo_de_pago=="rds" or tipo_de_pago=="rdm"):
            total_rds_array.append(Total_pedido)
            total_recogida_array.append(0)

        elif  grupo!=2 and trans_type=="pickup" and (tipo_de_pago=="cod" or tipo_de_pago=="obd"):
            total_recogida_array.append(Total_pedido)
            total_rds_array.append(0)

        else:
            total_rds_array.append(0)
            total_recogida_array.append(0)

        Descuentos= voucher_amnt + ptos_descuentos
        total_pedido_array.append(Total_pedido)
        total_producto_array.append(round(Total_prod,2))
        descuentos_array.append(Descuentos)
        coste_envios_array.append(delivery_charge)
        propinas_array.append(propina)
        Numero_de_pedidos_array.append(1)
        Negocios_ids_array.append(mid)
        comisiones_array.append(round(1.0 - com,2))
        grupos_array.append(grupo)
        n_cancelados.append(cancelado)
        n_completados.append(completado)


    dictionary = {'Negocio':hoja['Nombre del negocio'], 'Id negocio':Negocios_ids_array,'Grupo':grupos_array ,'Total pedido/Ticket': total_pedido_array,
    'Total producto': total_producto_array, 'Descuento':descuentos_array, 'Envio':coste_envios_array, 'Propinas':propinas_array,
    'Numero pedidos':Numero_de_pedidos_array, 'RDS':total_rds_array, 'Recogidas':total_recogida_array,'Repartidor/a':hoja['Repartidor/a'],
    'TransType':hoja['TransType'], 'Estado':hoja['Estado'], 'Tipo de pago':hoja['Tipo de pago'], 'Fecha de entrega':hoja['Fecha de entrega'], 'Order id':hoja['Order Id'],
    'Pedidos cancelados':n_cancelados, 'Pedidos completados':n_completados, 'Motivos': hoja['Motivos']}
    #17                                 18

    resultados= pd.DataFrame(dictionary)

    return resultados

def cambiar_precios(NOTA, Total_pedido,  Total_prod, voucher_amnt, ptos_descuentos, delivery_charge):
    """
    Realiza los cambios de precio
    Argumentos:
        -NOTA:nota del pedido
        -Total_pedido: total pedido/ticket del pedido
        -Total_prod:total producto del pedido
        -voucher_amnt: voucher amount del pedido
        -ptos_descuentos: puntos de descuento del pedido
        -delivery_charge: cargos de envío del pedido
    """
    nota=NOTA
    #Cambio las comas por puntos ya que Muchos precios en la notas suelen aparecer como 34,5 en vez de 34.5
    nota=nota.replace(",",".")
    nota=nota.replace("'",".")
    #Extraigo todos los numeros de la nota para comprobar el precio nuevo
    numeros=re.findall(r"[-+]?\d*\.\d+|\d+", nota);

    TP=Total_pedido

    if len(numeros)==1:
        Total_pedido=float(numeros[0])
        Total_prod=float(numeros[0])  + voucher_amnt + ptos_descuentos - delivery_charge

        if Total_prod<=0 and ('más' in nota or '+' in nota ):
            Total_pedido=TP + float(numeros[0])
            Total_prod=Total_pedido  + voucher_amnt + ptos_descuentos - delivery_charge

        elif Total_prod<=0:
            Total_pedido=float(TP) - float(numeros[0])
            Total_prod=Total_pedido  + voucher_amnt + ptos_descuentos - delivery_charge


        if Total_pedido>1000:
            Total_pedido=float(TP)
            Total_prod=Total_pedido  + voucher_amnt + ptos_descuentos - delivery_charge

    elif len(numeros)>1:
        pos=0
        for i in range(len(numeros)):
            if float(numeros[i])!=Total_pedido and float(numeros[i])<1000:
                Total_pedido=float(numeros[i])
                Total_prod=float(numeros[i])  + voucher_amnt + ptos_descuentos - delivery_charge
                pos=i
                break;

        if Total_prod<=0 and ('más' in nota or '+' in nota ):
            Total_pedido=TP + float(numeros[pos])
            Total_prod=Total_pedido  + voucher_amnt + ptos_descuentos - delivery_charge

        elif Total_prod<=0:
            Total_pedido=float(TP) - float(numeros[pos])
            Total_prod=Total_pedido  + voucher_amnt + ptos_descuentos - delivery_charge

    return Total_pedido,  Total_prod

def calcular_resultados_finales(resultados,dict_merchant_com, dict_merchant_city, dict_merchant_bank, dict_merchant_group, cancel):
    """
        Calculates the total benefits and the money Happy had payed from each merchant
        Argumentos:
            -resultados: dataframe de los pedidos obtenidos de la funcion 'operaciones()'
            -dict_merchant_com: diccionario con los ids y comisiones de cada negocio

        Devuelve un dataframe con los resultados totales de los negocios
    """

    motivos=resultados[ resultados['Pedidos cancelados']>0]
    motivos=motivos[['Negocio','Id negocio','Estado','Motivos']]
    motivos=motivos.dropna()
    n_motivos = motivos
    n_motivos=n_motivos.groupby(['Negocio','Id negocio'])['Motivos'].size().reset_index(name='Frecuencia')
    motivos.groupby(['Negocio','Id negocio'])['Motivos'].agg(pd.Series.mode)

    if cancel:
        copy=resultados[['Negocio', 'Id negocio', 'Pedidos cancelados', 'Pedidos completados']]
        copy=copy.groupby(['Negocio', 'Id negocio']).sum().reset_index()

        resultados=resultados[resultados['Pedidos cancelados']>0]
        resultados=resultados.groupby(['Negocio', 'Id negocio']).sum().reset_index()
        resultados['Pedidos completados']=copy['Pedidos completados']
    else:
        resultados=resultados[(resultados['Pedidos cancelados']==0) & (resultados['Pedidos completados']>0) ]
        resultados=resultados.groupby(['Negocio', 'Id negocio']).sum().reset_index()

    #Declaramos los arrays donde se guardan los resultados de las siguientes operaciones
    Nombres_negocios_array, Ticket_medio_array, Happy_paga_array, Cobro_envios_array, Cobro_recogidas_array, Beneficios_bruto_array, Beneficios_ticket_array, Reparto_tipo, Comisiones_array, Datos_BK_array, Grupos_array, Negocios_ids_array, Numero_de_pedidos_array, Ciudades, Motivos, P_cancelados, P_completados=([] for i in range(17))

    for x in range(len(resultados)):

        nombre=resultados.iloc[x,0]
        nombre=nombre.replace("&", "\&")
        nombre=nombre.replace("#", "\#")
        nombre=nombre.replace("$", "\$")
        nombre=nombre.replace("_", "\_")
        cantidad_pedidos=int(resultados.iloc[x,8])
        id=resultados.iloc[x,1]
        total_pedido=resultados.iloc[x,3]
        total_producto=resultados.iloc[x,4]
        descuentos=resultados.iloc[x,5]
        envios=resultados.iloc[x,6]
        total_rds=resultados.iloc[x,9]
        total_recogida=resultados.iloc[x,10]
        n_cancelados =resultados.iloc[x,12]
        n_completados=resultados.iloc[x,13]
        motivo='Nada'

        if nombre in motivos.values:
            fila=motivos.loc[motivos['Negocio'] == nombre]
            fil2=n_motivos.loc[n_motivos['Negocio'] == nombre]
            motivo= str(fil2['Frecuencia'].values[0]) + " veces: " + fila['Motivos'].values[0]

        #Accedemos la posición del negocio deseado para así saber la posición de su % de comision en el array comisiones, ya que tienen el mismo orden

        if cantidad_pedidos==0:
            Ticket_medio=0
        else:
            Ticket_medio=total_pedido/cantidad_pedidos

        com=dict_merchant_com[id]
        city=dict_merchant_city[id]
        dk=dict_merchant_bank[id]
        g=dict_merchant_group[id]

        #Si el id del negocio esta en NO_ENVIOS, es que tiene repartos externos, por tanto, se les hacen otras operaciones
        #Si no esta, es que tiene repartos internos
        if g==2:
            Happy_paga=total_producto*0.88
            B_bruto=total_producto-Happy_paga-descuentos
            Cobro_envios_array.append(total_rds-total_producto*0.12)
            Reparto_tipo.append(1)
            Cobro_recogidas_array.append(0)
            grupo="2"
            Comisiones_array.append(0.12)
        else:
            Comisiones_array.append(round(1.0 - com,2))
            Happy_paga=total_producto*float(com)
            B_bruto=total_producto-Happy_paga-descuentos + envios
            Reparto_tipo.append(0)
            Cobro_envios_array.append(0)
            if g==3:
                grupo="3"
                Cobro_recogidas_array.append(0)
            else:
                porcentaje=round(1.0 - com,2)
                Cobro_recogidas_array.append(Happy_paga - total_recogida + total_recogida*porcentaje)
                grupo=str(g)

        if cantidad_pedidos==0:
            B_ticket=0
        else:
            B_ticket=B_bruto/cantidad_pedidos

        #Este negocio en especifico siempre tiene 0 en happy_paga
        if id==ENVIOS_PARA_EMPRESAS_ID:
            Happy_paga_array.append(0)
        else:
            Happy_paga_array.append(Happy_paga)

        Ticket_medio_array.append(Ticket_medio)
        Beneficios_bruto_array.append(B_bruto)
        Beneficios_ticket_array.append(B_ticket)
        Nombres_negocios_array.append(nombre)
        Numero_de_pedidos_array.append(cantidad_pedidos)
        Datos_BK_array.append(dk)
        Grupos_array.append(grupo)
        Negocios_ids_array.append(id)
        Ciudades.append(city)
        Motivos.append(motivo)
        P_cancelados.append(n_cancelados)
        P_completados.append(n_completados)

    dictionary={'Negocio':Nombres_negocios_array,'Id negocio':Negocios_ids_array,'Comision':Comisiones_array,'Nº Bancarios':Datos_BK_array,
    'Total pedido/Ticket': resultados['Total pedido/Ticket'],'Total producto': resultados['Total producto'],'Descuento':resultados['Descuento'],'Envio':resultados['Envio'],
    'Propinas':resultados['Propinas'], 'Numero pedidos':Numero_de_pedidos_array,'Ticket medio':Ticket_medio_array, 'Happy paga':Happy_paga_array,'Beneficios bruto':Beneficios_bruto_array ,
    'Beneficios x ticket': Beneficios_ticket_array, 'Cobro de envios':Cobro_envios_array,'Cobro de recogidas': Cobro_recogidas_array,'Grupo':Grupos_array, 'Reparto':Reparto_tipo, 'Ciudad':Ciudades,
    'Pedidos cancelados':resultados['Pedidos cancelados'], 'Pedidos completados':resultados['Pedidos completados'], 'Motivos':Motivos}

    resultados_finales=pd.DataFrame(dictionary)
    resultados_finales=resultados_finales.round(decimals=2)

    return resultados_finales


def calcular_resultados_anteriores(fecha_inicio,fecha_final,dict_merchant_com, dict_merchant_group, ciudad, cancel):
    """
    Calculates the total benefits and the money Happy had payed from each merchant from the previous week,month or year orders
    Argumentos:
        -fecha_inicio: Fecha inicial del intervalo de tiempo del cual se extraen los pedidos
        -fecha_final: Fecha final del intervalo de tiempo del cual se extrean los pedidos. Si es none, solo es extraen los pedidos de la fecha fecha_inicio

    Devuelve el dataframe con los resultados totales de los pedidos, para compararlos en 'resultados_finales()'
    """
    assert fecha_final


    #print (fecha_inicio,fecha_final)

    hay_pedidos, pedidos=extraer_pedidos(fecha_inicio,fecha_final, ciudad)

    #dict_merchant_com= extraer_negocios_y_comisiones()
    if hay_pedidos:
        if not cancel:
            pedidos = pedidos.drop(pedidos[pedidos.Estado == 'cancelled'].index)
            pedidos = pedidos.drop(pedidos[pedidos.Estado == 'decline'].index)
            pedidos = pedidos.drop(pedidos[pedidos.Estado == 'declined'].index)
            pedidos = pedidos.drop(pedidos[pedidos.Estado == 'pending'].index)

        pxd=get_pedidos_por_dia(pedidos)
        pxh=get_pedidos_por_hora(pedidos)
        resultados=operaciones_por_pedido(pedidos, dict_merchant_com, dict_merchant_group)

        if cancel:
            copy=resultados[['Negocio', 'Pedidos cancelados', 'Pedidos completados']]
            copy=copy.groupby(['Negocio']).sum().reset_index()

            resultados=resultados[(resultados['Estado']=='pending') | (resultados['Estado']=='cancelled') | (resultados['Estado']=='decline') | (resultados['Estado']=='declined')]
            resultados=resultados.groupby(['Negocio', 'Id negocio']).sum().reset_index()
            resultados['Pedidos cancelados']=copy['Pedidos cancelados']
            resultados['Pedidos completados']=copy['Pedidos completados']

            if resultados['Pedidos cancelados'].sum()==0:
                nada = [0] * 1
                algo= [0] * 1
                dictionary={'Total pedido/Ticket': nada, 'Numero pedidos':algo,
                'Ticket medio':nada,'Beneficios bruto':nada, 'Beneficios x ticket': nada,'Pedidos cancelados':algo, 'Pedidos completados': algo}

                resultados=pd.DataFrame(dictionary)
                f_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d')
                f_final = datetime.strptime(fecha_final, '%Y-%m-%d')

                delta= f_final- f_inicio
                intervalo=delta.days
                if intervalo>7:
                    intervalo=7
                pxd=[0] * int(intervalo)
                pxh=[0] * 14
                #print(resultados.sum())
                return resultados.sum(), pxd, pxh

        else:
            resultados=resultados.groupby(['Negocio', 'Id negocio']).sum().reset_index()


        Ticket_medio_array, Happy_paga_array, Beneficios_bruto_array, Beneficios_ticket_array, Numero_de_pedidos_array=([] for i in range(5))

        for x in range(len(resultados)):

            nombre=resultados.iloc[x,0]
            cantidad_pedidos=int(resultados.iloc[x,8])
            id=resultados.iloc[x,1]
            total_pedido=resultados.iloc[x,3]
            total_producto=resultados.iloc[x,4]
            descuentos=resultados.iloc[x,5]
            envios=resultados.iloc[x,6]

            com=dict_merchant_com[id]
            g=dict_merchant_group[id]

            Ticket_medio=total_pedido/cantidad_pedidos

            if g==2:
                Happy_paga=total_producto*0.88
                B_bruto=total_producto-Happy_paga-descuentos
            else:
                Happy_paga=total_producto*float(com)
                B_bruto=total_producto-Happy_paga-descuentos + envios


            B_ticket=B_bruto/cantidad_pedidos

            if id==113:
                Happy_paga_array.append(0)
            else:
                Happy_paga_array.append(Happy_paga)

            Ticket_medio_array.append(Ticket_medio)
            Beneficios_bruto_array.append(B_bruto)
            Beneficios_ticket_array.append(B_ticket)
            Numero_de_pedidos_array.append(cantidad_pedidos)


        #[['Total pedido/Ticket', 'Numero pedidos', 'Ticket medio', 'Beneficios bruto','Beneficios x ticket', 'Pedidos cancelados', 'Pedidos completados']]
        dictionary={'Total pedido/Ticket': resultados['Total pedido/Ticket'], 'Numero pedidos':Numero_de_pedidos_array,
        'Ticket medio':Ticket_medio_array,'Beneficios bruto':Beneficios_bruto_array, 'Beneficios x ticket': Beneficios_ticket_array,
        'Pedidos cancelados':resultados['Pedidos cancelados'], 'Pedidos completados':resultados['Pedidos completados']}

        resultados=pd.DataFrame(dictionary)

        return resultados.sum() , pxd, pxh

    else:
        nada = [0] * 1
        algo= [0] * 1
        dictionary={'Total pedido/Ticket': nada, 'Numero pedidos':algo,
        'Ticket medio':nada,'Beneficios bruto':nada, 'Beneficios x ticket': nada,'Pedidos cancelados':algo, 'Pedidos completados': algo}

        resultados=pd.DataFrame(dictionary)
        f_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d')
        f_final = datetime.strptime(fecha_final, '%Y-%m-%d')
        delta= f_final- f_inicio
        intervalo=delta.days
        if intervalo>7:
            intervalo=7
        pxd=[0] * int(intervalo)
        pxh=[0] * 14
        #print(resultados.sum())
        return resultados.sum(), pxd, pxh

def get_datos_anteriores(tipo,fecha_inicio, fecha_final, dict_merchant_com, dict_merchant_group, ciudad, cancel, pedidos_por_dia, pedidos_por_hora):

    if tipo==1:

        f_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d') + timedelta(days=-7)
        f_inicio =f_inicio.strftime('%Y-%m-%d')
        f_final = datetime.strptime(fecha_final, '%Y-%m-%d') + timedelta(days=-7)
        f_final =f_final.strftime('%Y-%m-%d')

        SUM, pxd_sem_ant, pxh_sem_ant=calcular_resultados_anteriores(f_inicio, f_final, dict_merchant_com, dict_merchant_group, ciudad, cancel)
        clientes_sem_ant=get_clientes_registrados(f_inicio, f_final)
        clientes_sem_act=get_clientes_registrados(fecha_inicio, fecha_final)
        reg_clientes=[clientes_sem_ant, clientes_sem_act]
        crear_graf_barras_clientes(reg_clientes, tipo)

        pxd=[pxd_sem_ant, pedidos_por_dia]
        crear_graf_pedidosxdia(pxd, tipo)

        pxh=[pxh_sem_ant, pedidos_por_hora]
        crear_graf_pedidosxhora(pxh, tipo)

        return SUM

    elif tipo==2:
        f_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d') + relativedelta(months=-1)
        f_inicio = f_inicio.strftime('%Y-%m-%d')
        f_final = datetime.strptime(fecha_final, '%Y-%m-%d') + relativedelta(months=-1)
        f_final = f_final.strftime('%Y-%m-%d')

        clientes_mes_ant= get_clientes_registrados(f_inicio, f_final)
        SUM_mes, pxd_mes_ant, pxh_mes_ant=calcular_resultados_anteriores(f_inicio, f_final, dict_merchant_com, dict_merchant_group, ciudad, cancel)

        f_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d') + relativedelta(years=-1)
        f_inicio = f_inicio.strftime('%Y-%m-%d')
        f_final = datetime.strptime(fecha_final, '%Y-%m-%d') + relativedelta(years=-1)
        f_final = f_final.strftime('%Y-%m-%d')

        SUM_anio, pxd_anio_ant, pxh_anio_ant=calcular_resultados_anteriores(f_inicio,f_final,dict_merchant_com,dict_merchant_group, ciudad, cancel)

        clientes_anio_ant= get_clientes_registrados(f_inicio, f_final)
        clientes_mes_act=get_clientes_registrados(fecha_inicio, fecha_final)
        reg_clientes=[clientes_anio_ant, clientes_mes_ant, clientes_mes_act]
        crear_graf_barras_clientes(reg_clientes, tipo)

        pxd=[pxd_anio_ant, pxd_mes_ant, pedidos_por_dia]
        crear_graf_pedidosxdia(pxd, tipo)

        pxh=[pxh_anio_ant, pxh_mes_ant, pedidos_por_hora]
        crear_graf_pedidosxhora(pxh, tipo)

        return SUM_mes, SUM_anio

    elif tipo==5:
        f_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d') + relativedelta(years=-1)
        f_inicio = f_inicio.strftime('%Y-%m-%d')
        f_final = datetime.strptime(fecha_final, '%Y-%m-%d') + relativedelta(years=-1)
        f_final = f_final.strftime('%Y-%m-%d')

        SUM_anio, pxd_anio_ant, pxh_anio_ant=calcular_resultados_anteriores(f_inicio,f_final,dict_merchant_com,dict_merchant_group, ciudad, cancel)

        clientes_anio_ant= get_clientes_registrados(f_inicio, f_final)
        clientes_anio_act=get_clientes_registrados(fecha_inicio, fecha_final)
        reg_clientes=[clientes_anio_ant, clientes_anio_act]
        crear_graf_barras_clientes(reg_clientes, tipo)

        pxd=[pxd_anio_ant,  pedidos_por_dia]
        crear_graf_pedidosxdia(pxd, tipo)

        pxh=[pxh_anio_ant, pedidos_por_hora]
        crear_graf_pedidosxhora(pxh, tipo)

        return SUM_anio


def exportar_informe(resultados, dict_merchant_com, dict_merchant_group, fecha_inicio, fecha_final, pedidos_por_dia, pxh, conductores, ciudad, cancel):

    """
    Calculates the total benefits and the money Happy had payed from each merchant
    Argumentos:
        -resultados: dataframe de los pedidos obtenidos de la funcion 'operaciones()'
        -dict_merchant_com: diccionario con los ids y comisiones de cada negocio
        -fecha_inicio: Fecha inicial del intervalo de tiempo del cual se extraen los pedidos
        -fecha_final: Fecha final del intervalo de tiempo del cual se extrean los pedidos. Si es none, solo es extraen los pedidos de la fecha fecha_inicio
        -pedidos_por_dia: dataframe con el numeor de pedidos realizados cada dia durante ese intervalo de tiempo
        -conductores: dataframe con el numero de pedidos realizados por cada repartidor/a
        -web_vs_app: dataframe que indica el numero de pedidos realizados en la web y en la app

    """
    Fechas=[fecha_inicio, fecha_final]
    resultados = resultados.astype({'Id negocio':str ,"Comision": str})

    r_interno=resultados[resultados['Reparto']==0]
    r_interno=r_interno.drop(columns=['Cobro de envios','Reparto'])
    r_interno.loc['Total']= r_interno.sum(numeric_only=True, axis=0)
    r_interno.at['Total','Ticket medio']= r_interno.at['Total','Total pedido/Ticket']/r_interno.at['Total','Numero pedidos']
    r_interno.at['Total','Negocio']="Total"
    r_interno.at['Total','Beneficios x ticket']= r_interno.at['Total','Beneficios bruto']/r_interno.at['Total','Numero pedidos']
    r_interno=r_interno.round(decimals=2)
    r_interno=r_interno.rename(columns={'Negocio': 'Repartos int'})
    r_interno['Pedidos completados']=r_interno['Pedidos completados'].round()
    r_interno = r_interno.astype({ 'Numero pedidos': int, 'Pedidos cancelados': int, 'Pedidos completados': int})
    r_interno = r_interno.fillna("")


    r_externo=resultados[resultados['Reparto']==1]
    r_externo=r_externo.drop(columns=['Cobro de recogidas','Reparto'])
    r_externo.loc['Total']= r_externo.sum(numeric_only=True, axis=0)
    r_externo.at['Total','Ticket medio']= r_externo.at['Total','Total pedido/Ticket']/r_externo.at['Total','Numero pedidos']
    r_externo.at['Total','Negocio']="Total"
    r_externo.at['Total','Beneficios x ticket']= r_externo.at['Total','Beneficios bruto']/r_externo.at['Total','Numero pedidos']
    r_externo=r_externo.round(decimals=2)
    r_externo=r_externo.rename(columns={'Negocio': 'Repartos ext'})
    r_externo['Pedidos completados']=r_externo['Pedidos completados'].round()
    r_externo = r_externo.astype({ 'Numero pedidos': int, 'Pedidos cancelados': int, 'Pedidos completados': int})
    r_externo = r_externo.fillna("")


    resultados=resultados.drop(columns=['Cobro de envios','Cobro de recogidas','Reparto'])
    resultados.loc['Total']= resultados.sum(numeric_only=True, axis=0)
    resultados.at['Total','Ticket medio']= resultados.at['Total','Total pedido/Ticket']/resultados.at['Total','Numero pedidos']
    resultados.at['Total','Beneficios x ticket']= resultados.at['Total','Beneficios bruto']/resultados.at['Total','Numero pedidos']
    resultados.at['Total','Numero pedidos']= int(resultados.at['Total','Numero pedidos'])
    resultados=resultados[['Total pedido/Ticket', 'Numero pedidos', 'Ticket medio', 'Beneficios bruto','Beneficios x ticket', 'Pedidos cancelados', 'Pedidos completados']]

    r_interno = r_interno.astype({ "Numero pedidos": int})
    r_externo = r_externo.astype({ "Numero pedidos": int})

    imagen='logo.png'
    tipo=0


    if fecha_final:

        f_inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d')
        f_final = datetime.strptime(fecha_final, '%Y-%m-%d')

        delta= f_final- f_inicio

        #Si el intervalo es de una semana, se comparan con los resultados de la semana anterior
        if delta.days<=7 and delta.days>0:
            tipo=1

            resultados.loc['Total semana anterior']=get_datos_anteriores(tipo,fecha_inicio, fecha_final, dict_merchant_com, dict_merchant_group, ciudad, cancel, pedidos_por_dia, pxh)
            anterior=resultados.at['Total semana anterior','Numero pedidos']
            clientes_sem_ant=get_clientes_registrados(f_inicio, f_final)
            if anterior!=0:
                resultados.at['Total semana anterior','Ticket medio']= resultados.at['Total semana anterior','Total pedido/Ticket']/anterior
                resultados.at['Total semana anterior','Beneficios x ticket']= resultados.at['Total semana anterior','Beneficios bruto']/anterior

                resultados.loc['Total semana anterior']=resultados.loc['Total semana anterior'].replace(0.0, 0.9)
                resultados.loc['Comparacion']= (resultados.loc['Total']/resultados.loc['Total semana anterior'] - 1)*100
                resultados.loc['Total semana anterior']=resultados.loc['Total semana anterior'].replace(0.9, 0.0)

            else:
                resultados.at['Total semana anterior','Ticket medio']= 0
                resultados.at['Total semana anterior','Beneficios x ticket']= 0
                resultados.loc['Comparacion']= resultados.loc['Total']

            resultados=resultados.round(decimals=2)
            resultados['Numero pedidos']=resultados['Numero pedidos'].round()
            resultados = resultados.fillna(0)
            resultados = resultados.astype({ 'Numero pedidos': int, 'Pedidos cancelados': int, 'Pedidos completados': int})
            TOTAL=resultados.tail(3)

            N="Informe_semanal_"+fecha_inicio+"_"+ciudad

        #Si el intervalo es mayor de una semana, se comparan con los resultados del mes y año anterior
        elif delta.days>7 and delta.days<32:
            tipo=2

            resultados.loc['Total mes anterior'], resultados.loc['Total anio anterior']=get_datos_anteriores(tipo,fecha_inicio, fecha_final, dict_merchant_com, dict_merchant_group, ciudad, cancel, pedidos_por_dia, pxh)
            anterior=resultados.at['Total mes anterior','Numero pedidos']

            if anterior!=0:
                resultados.at['Total mes anterior','Ticket medio']= resultados.at['Total mes anterior','Total pedido/Ticket']/anterior
                resultados.at['Total mes anterior','Beneficios x ticket']= resultados.at['Total mes anterior','Beneficios bruto']/anterior

                resultados.loc['Total mes anterior']=resultados.loc['Total mes anterior'].replace(0.0, 0.9)
                resultados.loc['Comparacion mes anterior']=(resultados.loc['Total']/resultados.loc['Total mes anterior'] - 1)*100
                resultados.loc['Total mes anterior']=resultados.loc['Total mes anterior'].replace(0.9, 0.0)

            else:
                resultados.at['Total mes anterior','Ticket medio']= 0
                resultados.at['Total mes anterior','Beneficios x ticket']= 0
                resultados.loc['Comparacion mes anterior']=resultados.loc['Total']

            anterior=resultados.at['Total anio anterior','Numero pedidos']

            if anterior!=0:
                resultados.at['Total anio anterior','Ticket medio']= resultados.at['Total anio anterior','Total pedido/Ticket']/anterior
                resultados.at['Total anio anterior','Beneficios x ticket']= resultados.at['Total anio anterior','Beneficios bruto']/anterior

                resultados.loc['Total anio anterior']=resultados.loc['Total anio anterior'].replace(0.0, 0.9)
                resultados.loc['Comparacion anio anterior']=(resultados.loc['Total']/resultados.loc['Total anio anterior'] - 1)*100
                resultados.loc['Total anio anterior']=resultados.loc['Total anio anterior'].replace(0.9, 0.0)

            else:

                resultados.at['Total anio anterior','Ticket medio']= 0
                resultados.at['Total anio anterior','Beneficios x ticket']= 0
                resultados.loc['Comparacion anio anterior']=resultados.loc['Total']

            resultados=resultados.round(decimals=2)
            resultados['Numero pedidos']=resultados['Numero pedidos'].round()
            resultados = resultados.fillna(0)
            resultados = resultados.astype({ 'Numero pedidos': int, 'Pedidos cancelados': int, 'Pedidos completados': int})
            TOTAL=resultados.tail(5)

            N="Informe_mensual_"+fecha_inicio+"_"+ciudad

        elif delta.days>32 and delta.days<366:
            tipo=5

            resultados.loc['Total anio anterior']=get_datos_anteriores(tipo,fecha_inicio, fecha_final, dict_merchant_com, dict_merchant_group, ciudad, cancel, pedidos_por_dia, pxh)
            anterior=resultados.at['Total anio anterior','Numero pedidos']

            if anterior!=0:
                resultados.at['Total anio anterior','Ticket medio']= resultados.at['Total anio anterior','Total pedido/Ticket']/anterior
                resultados.at['Total anio anterior','Beneficios x ticket']= resultados.at['Total anio anterior','Beneficios bruto']/anterior

                resultados.loc['Total anio anterior']=resultados.loc['Total anio anterior'].replace(0.0, 0.9)
                resultados.loc['Comparacion anio anterior']=(resultados.loc['Total']/resultados.loc['Total anio anterior'] - 1)*100
                resultados.loc['Total anio anterior']=resultados.loc['Total anio anterior'].replace(0.9, 0.0)

            else:

                resultados.at['Total anio anterior','Ticket medio']= 0
                resultados.at['Total anio anterior','Beneficios x ticket']= 0
                resultados.loc['Comparacion anio anterior']=resultados.loc['Total']

            resultados=resultados.round(decimals=2)
            resultados['Numero pedidos']=resultados['Numero pedidos'].round()
            resultados = resultados.fillna(0)
            resultados = resultados.astype({ 'Numero pedidos': int, 'Pedidos cancelados': int, 'Pedidos completados': int})
            TOTAL=resultados.tail(3)

            N="Informe_anual_"+fecha_inicio+"_"+ciudad
        else:
            exit()
    else:
        tipo=3
        resultados=resultados.round(decimals=2)
        resultados['Numero pedidos']=resultados['Numero pedidos'].round()
        resultados = resultados.astype({ 'Numero pedidos': int, 'Pedidos cancelados': int, 'Pedidos completados': int})
        TOTAL=resultados.tail(1)

        N="Informe_diario_"+fecha_inicio+"_"+ciudad


    if cancel:
        tipo=4
        N=N.replace("Informe", "Informe_cancelaciones")

    export_to_excel(r_interno, r_externo, TOTAL, conductores, N, tipo)
    export_to_latex(r_interno, r_externo, TOTAL, conductores,N, Fechas, tipo , ciudad)



def export_to_excel(r_internol, r_externo, TOTAL, conductores, N, tipo):



    workbook = xlsxwriter.Workbook(N+'_Comparativas.xlsx')
    worksheet = workbook.add_worksheet()
    if tipo==1:
        worksheet.insert_image('A1', 'pedidos_por_dia.png')
        worksheet.insert_image('K1', 'comparacion_registros_clientes.png')

        worksheet.insert_image('A20', 'tipo_pago_count.png')
        worksheet.insert_image('K20', 'web_vs_app.png')

        worksheet.insert_image('A37', 'completados_vs_cancelados.png')
        worksheet.insert_image('K37', 'pedidos_por_hora.png')

        #file1 = open("plantilla_semanal.tex", 'r')
    elif tipo==2:
        worksheet.insert_image('A1', 'pedidos_por_dia.png')
        worksheet.insert_image('K1', 'comparacion_registros_clientes.png')

        worksheet.insert_image('A20', 'tipo_pago_count.png')
        worksheet.insert_image('K20', 'web_vs_app.png')

        worksheet.insert_image('A37', 'completados_vs_cancelados.png')
        worksheet.insert_image('K37', 'pedidos_por_hora.png')
        #file1 = open("plantilla_mensual.tex", 'r')
    elif tipo==3 or tipo==5:
        worksheet.insert_image('A1', 'web_vs_app.png')
        worksheet.insert_image('K1', 'tipo_pago_count.png')

        worksheet.insert_image('A20', 'completados_vs_cancelados.png')
    else:
        worksheet.insert_image('A1', 'completados_vs_cancelados.png')

        #file1 = open("plantilla_cancel.tex", 'r')


    workbook.close()

    with pd.ExcelWriter(N+'.xlsx') as writer:
        r_internol.to_excel(writer, sheet_name='Repartos internos')
        r_externo.to_excel(writer, sheet_name='Repartos externos')
        TOTAL.to_excel(writer, sheet_name='Total')
        if conductores is not None:
            conductores.to_excel(writer, sheet_name='Repartidores')

def export_to_latex(r_internol, r_externo, TOTAL, conductores, N, Fechas, tipo, ciudad):

    if tipo==1:
        file1 = open("plantilla_semanal.tex", 'r')
    elif tipo==2:
        file1 = open("plantilla_mensual.tex", 'r')
    elif tipo==3:
        file1 = open("plantilla_diaria.tex", 'r')
    elif tipo==5:
        file1 = open("plantilla_anual.tex", 'r')
    else:
        file1 = open("plantilla_cancel.tex", 'r')

    #file1 = open("plantilla_semanal.tex", 'r')
    Lines = file1.readlines()

    with open(N+".tex", 'w', encoding='utf8') as _file:
        for line in Lines:
            Par=True
            if line.strip()=='%INT':

               for x in range(len(r_internol)):
                   nombre=str(r_internol.iloc[x,0])
                   id=str(r_internol.iloc[x,1])
                   com=str(r_internol.iloc[x,2])
                   datos_bnk=str(r_internol.iloc[x,3])
                   t_pedido=str(r_internol.iloc[x,4])+'€'
                   t_producto=str(r_internol.iloc[x,5])+'€'
                   desc=str(r_internol.iloc[x,6])+'€'
                   envio=str(r_internol.iloc[x,7])+'€'
                   propina=str(r_internol.iloc[x,8])+'€'
                   n_pedidos=str(r_internol.iloc[x,9])
                   t_medio=str(r_internol.iloc[x,10])+'€'
                   h_paga=str(r_internol.iloc[x,11])+'€'
                   b_bruto=str(r_internol.iloc[x,12])+'€'
                   b_ticket=str(r_internol.iloc[x,13])+'€'
                   c_recogidas=str(r_internol.iloc[x,14])+'€'
                   grupo=str(r_internol.iloc[x,15])
                   city=str(r_internol.iloc[x,16])

                   S1= id +' & '+  nombre + ' & '+ grupo + ' & '+com+ ' & '+datos_bnk+ ' & '+ n_pedidos+ ' & '+ t_pedido+ ' & '+ t_producto + ' & '+desc+ ' & '
                   S2= envio+ ' & '+ propina+ ' & '+ t_medio + ' & '+h_paga + ' & '+b_bruto + ' & '+b_ticket + ' & '+c_recogidas + ' & '+ city + "\\" +"\\"
                   color=''

                   if Par:
                       Par=False
                   else:
                       Par=True
                       color='\\rowcolor{lightgray} '

                   if nombre=="Total":
                     color='\\rowcolor{total} '
                     #_file.writelines("\n")

                   S=color+S1+S2
                   _file.writelines(S )
                   _file.writelines("\n")
                   _file.writelines("\hline")
                   _file.writelines("\n")

            elif line.strip()=='%EXT':
                for x in range(len(r_externo)):
                    nombre=str(r_externo.iloc[x,0])
                    id=str(r_externo.iloc[x,1])
                    com=str(r_externo.iloc[x,2])
                    datos_bnk=str(r_externo.iloc[x,3])
                    t_pedido=str(r_externo.iloc[x,4])+'€'
                    t_producto=str(r_externo.iloc[x,5])+'€'
                    desc=str(r_externo.iloc[x,6])+'€'
                    envio=str(r_externo.iloc[x,7])+'€'
                    propina=str(r_externo.iloc[x,8])+'€'
                    n_pedidos=str(r_externo.iloc[x,9])
                    t_medio=str(r_externo.iloc[x,10])+'€'
                    h_paga=str(r_externo.iloc[x,11])+'€'
                    b_bruto=str(r_externo.iloc[x,12])+'€'
                    b_ticket=str(r_externo.iloc[x,13])+'€'
                    c_envios=str(r_externo.iloc[x,14])+'€'
                    grupo=str(r_externo.iloc[x,15])
                    city=str(r_externo.iloc[x,16])

                    S1= id +' & '+  nombre + ' & '+ grupo + ' & '+ com + ' & '+ datos_bnk + ' & '+ n_pedidos + ' & '+ t_pedido+ ' & '+ t_producto + ' & '+ desc + ' & '
                    S2= envio + ' & '+ propina + ' & '+ t_medio + ' & '+h_paga + ' & '+b_bruto + ' & '+ b_ticket + ' & '+ c_envios + ' & '+ city +"\\" +"\\"

                    color=''

                    if Par:
                        Par=False
                    else:
                        Par=True
                        color='\\rowcolor{lightgray} '

                    if nombre=="Total":
                      color='\\rowcolor{total} '

                    S=color+S1+S2
                    _file.writelines(S )
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")

            elif line.strip()=='%REPNUM':
                for x in range(len(conductores)):
                     S="\textbf{"+ str(conductores.iloc[x,0])+'} & \textbf{'+ str(conductores.iloc[x,1]) +'} & '+ str(conductores.iloc[x,2]) +' & '+ str(conductores.iloc[x,3])+' & '+ str(conductores.iloc[x,4])+ "\\" +"\\"

                     _file.writelines(S )
                     _file.writelines("\n")
                     _file.writelines("\hline")
                     _file.writelines("\n")

            elif line.strip()=='%TOTAL':
                if len(TOTAL)==3:
                    S= 'Total & '+ str(TOTAL.iloc[0,0]) +' & '+ str(TOTAL.iloc[0,1]) + ' & '+ str(TOTAL.iloc[0,2]) + ' & '+ str(TOTAL.iloc[0,3]) + ' & '+ str(TOTAL.iloc[0,4]) + "\\" +"\\"
                    if tipo==5:
                        S1= 'Total anio anterior & '+ str(TOTAL.iloc[1,0]) +' & '+ str(TOTAL.iloc[1,1]) + ' & '+ str(TOTAL.iloc[1,2]) + ' & '+ str(TOTAL.iloc[1,3]) + ' & '+ str(TOTAL.iloc[1,4]) + "\\" +"\\"
                    else:
                        S1= 'Total semana anterior & '+ str(TOTAL.iloc[1,0]) +' & '+ str(TOTAL.iloc[1,1]) + ' & '+ str(TOTAL.iloc[1,2]) + ' & '+ str(TOTAL.iloc[1,3]) + ' & '+ str(TOTAL.iloc[1,4]) + "\\" +"\\"


                    _file.writelines(S )
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")
                    _file.writelines(S1 )
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")

                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")
                    n=len(TOTAL)-1
                    c1=TOTAL.iloc[n,0]
                    c2=TOTAL.iloc[n,1]
                    c3=TOTAL.iloc[n,2]
                    c4=TOTAL.iloc[n,3]
                    c5=TOTAL.iloc[n,4]

                    C=[c1,c2,c3,c4,c5]

                    for i in range(len(C)):
                        if C[i]>0:
                            C[i]="\\colorbox{pos}{"+str(C[i])+"}"
                        elif C[i]<0:
                            C[i]="\\colorbox{neg}{"+str(C[i])+"}"
                        else:
                            C[i]=str(C[i])

                    if tipo==5:
                        _file.writelines('Comparacion anio anterior & '+ str(C[0]) +' & '+ str(C[1]) + ' & '+ str(C[2]) + ' & '+ str(C[3]) + ' & '+ str(C[4]) + "\\" +"\\")
                    else:
                        _file.writelines('Comparacion semana anterior & '+ str(C[0]) +' & '+ str(C[1]) + ' & '+ str(C[2]) + ' & '+ str(C[3]) + ' & '+ str(C[4]) + "\\" +"\\")

                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")

                elif len(TOTAL)==5:

                    S= 'Total & '+ str(TOTAL.iloc[0,0]) +'€ & '+ str(TOTAL.iloc[0,1]) + ' & '+ str(TOTAL.iloc[0,2]) + '€ & '+ str(TOTAL.iloc[0,3]) + '€ & '+ str(TOTAL.iloc[0,4]) + "\\" +"\\"

                    _file.writelines(S )
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")

                    S= 'Total mes anterior & '+ str(TOTAL.iloc[1,0]) +'€ & '+ str(TOTAL.iloc[1,1]) + ' & '+ str(TOTAL.iloc[1,2]) + '€ & '+ str(TOTAL.iloc[1,3]) + '€ & '+ str(TOTAL.iloc[1,4]) + "\\" +"\\"

                    _file.writelines(S )
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")
                    n=3
                    C=[TOTAL.iloc[n,0], TOTAL.iloc[n,1], TOTAL.iloc[n,2], TOTAL.iloc[n,3], TOTAL.iloc[n,4]]

                    for i in range(len(C)):
                         if C[i]>0:
                             C[i]="\\colorbox{pos}{"+str(C[i])+"}"
                         elif C[i]<0:
                             C[i]="\\colorbox{neg}{"+str(C[i])+"}"
                         else:
                             C[i]=str(C[i])

                    _file.writelines('Comparacion mes anterior & '+ str(C[0]) +' & '+ str(C[1]) + ' & '+ str(C[2]) + ' & '+ str(C[3]) + ' & '+ str(C[4]) + "\\" +"\\")
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")

                    S= 'Total anio anterior & '+ str(TOTAL.iloc[2,0]) +'€ & '+ str(TOTAL.iloc[2,1]) + ' & '+ str(TOTAL.iloc[2,2]) + '€ & '+ str(TOTAL.iloc[2,3]) + '€ & '+ str(TOTAL.iloc[2,4]) + "\\" +"\\"

                    _file.writelines(S )
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")

                    n=4
                    C=[TOTAL.iloc[n,0],TOTAL.iloc[n,1],TOTAL.iloc[n,2],TOTAL.iloc[n,3],TOTAL.iloc[n,4]]
                    for i in range(len(C)):
                        if C[i]>0:
                            C[i]="\\colorbox{pos}{"+str(C[i])+"}"
                        elif C[i]<0:
                            C[i]="\\colorbox{neg}{"+str(C[i])+"}"
                        else:
                            C[i]=str(C[i])

                    _file.writelines('Comparacion anio anterior & '+ str(C[0]) +' & '+ str(C[1]) + ' & '+ str(C[2]) + ' & '+ str(C[3]) + ' & '+ str(C[4]) + "\\" +"\\")


                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")
                else:
                    S= 'Total & '+ str(TOTAL.iloc[0,0]) +'€  & '+ str(TOTAL.iloc[0,1]) + ' & '+ str(TOTAL.iloc[0,2]) + '€ & '+ str(TOTAL.iloc[0,3]) + '€ & '+ str(TOTAL.iloc[0,4]) + "€ \\" +"\\"

                    _file.writelines(S )
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")

            elif line.strip()=='%FECHA':
                if Fechas[1]:
                    f=str(Fechas[0])+" - "+str(Fechas[1])
                else:
                    f=str(Fechas[0])

                _file.writelines("\\textbf{"+ciudad+"} \\")
                _file.writelines("\n")
                _file.writelines("\\textbf{"+f+"}")
                _file.writelines("\n")

            elif line.strip()=='%CINT':

               for x in range(len(r_internol)):
                   nombre=str(r_internol.iloc[x,0])
                   id=str(r_internol.iloc[x,1])
                   t_pedido=str(r_internol.iloc[x,4])+'€'
                   envio=str(r_internol.iloc[x,7])+'€'
                   h_paga=str(r_internol.iloc[x,11])+'€'
                   b_bruto=str(r_internol.iloc[x,12])+'€'
                   grupo=str(r_internol.iloc[x,15])
                   motivo=str(r_internol.iloc[x,19])
                   n_cancelados=str(r_internol.iloc[x,17])
                   n_completados=str(r_internol.iloc[x,18])

                   S1= id +' & '+  nombre + ' & '+ grupo + ' & '+n_cancelados+ ' & '+n_completados+ ' & \cellcolor{neg}'+ t_pedido+ ' & \cellcolor{orange}'+ envio +  ' & \cellcolor{green} '
                   S2= h_paga + ' & \cellcolor{green} '+ b_bruto + ' & ' + motivo + "\\" +"\\"
                   color=''
                    #Id negocio & Nombre & Grupo & Pedidos cancelados & Pedidos completados & Total pedido/Ticket & Envio & Happy paga &	Beneficios Happy & Motivos
                   if Par:
                       Par=False
                   else:
                       Par=True
                       color='\\rowcolor{lightgray} '

                   if nombre=="Total":
                     color='\\rowcolor{total} '
                     #_file.writelines("\n")

                   S=color+S1+S2
                   _file.writelines(S )
                   _file.writelines("\n")
                   _file.writelines("\hline")
                   _file.writelines("\n")

            elif line.strip()=='%CEXT':

              for x in range(len(r_externo)):
                  nombre=str(r_externo.iloc[x,0])
                  id=str(r_externo.iloc[x,1])
                  t_pedido=str(r_externo.iloc[x,4])+'€'
                  envio=str(r_externo.iloc[x,7])+'€'
                  h_paga=str(r_externo.iloc[x,11])+'€'
                  b_bruto=str(r_externo.iloc[x,12])+'€'
                  grupo=str(r_externo.iloc[x,15])
                  motivo=str(r_externo.iloc[x,19])
                  n_cancelados=str(r_externo.iloc[x,17])
                  n_completados=str(r_externo.iloc[x,18])

                  S1= id +' & '+  nombre + ' & '+ grupo + ' & '+n_cancelados+ ' & '+n_completados+ ' & \cellcolor{neg}'+ t_pedido+ ' & \cellcolor{orange}'+ envio +  ' & \cellcolor{green} '
                  S2= h_paga + ' & \cellcolor{green} '+ b_bruto + ' & ' + motivo + "\\" +"\\"
                  color=''
                   #Id negocio & Nombre & Grupo & Pedidos cancelados & Pedidos completados & Total pedido/Ticket & Envio & Happy paga &	Beneficios Happy & Motivos
                  if Par:
                      Par=False
                  else:
                      Par=True
                      color='\\rowcolor{lightgray} '

                  if nombre=="Total":
                    color='\\rowcolor{total} '
                    #_file.writelines("\n")

                  S=color+S1+S2
                  _file.writelines(S )
                  _file.writelines("\n")
                  _file.writelines("\hline")
                  _file.writelines("\n")

            elif line.strip()=='%CTOTAL':

                if len(TOTAL)==3:

                    S= 'Total & '+ str(TOTAL.iloc[0,0]) +' & '+ str(TOTAL.iloc[0,5]) + ' & '+ str(TOTAL.iloc[0,6]) + ' & '+ str(TOTAL.iloc[0,3])  + "€ \\" +"\\"
                    if tipo==5:
                        S1= 'Total anio anterior & '+ str(TOTAL.iloc[1,0]) +' & '+ str(TOTAL.iloc[1,3]) + ' & '+ str(TOTAL.iloc[1,6]) + ' & '+ str(TOTAL.iloc[1,3]) + " € \\" +"\\"
                    else:
                        S1= 'Total semana anterior & '+ str(TOTAL.iloc[1,0]) +' & '+ str(TOTAL.iloc[1,3]) + ' & '+ str(TOTAL.iloc[1,6]) + ' & '+ str(TOTAL.iloc[1,3]) + " € \\" +"\\"

                    _file.writelines(S )
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")
                    _file.writelines(S1 )
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")

                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")
                    n=len(TOTAL)-1
                    c1=TOTAL.iloc[n,0]
                    c2=TOTAL.iloc[n,5]
                    c3=TOTAL.iloc[n,6]
                    c4=TOTAL.iloc[n,3]

                    C=[c1,c2,c3,c4]

                    for i in range(len(C)):
                        if (C[i]>0 and i==2) or (C[i]<0 and i!=2):
                            C[i]="\\colorbox{pos}{"+str(C[i])+"}"
                        elif (C[i]<0 and i==2) or (C[i]>0 and i!=2):
                            C[i]="\\colorbox{neg}{"+str(C[i])+"}"
                        else:
                            C[i]=str(C[i])


                    if tipo==5:
                        _file.writelines('Comparacion anio anterior & '+ str(C[0]) +' & '+ str(C[1]) + ' & '+ str(C[2]) + ' & '+ str(C[3]) + "\\" +"\\")
                    else:
                        _file.writelines('Comparacion semana anterior & '+ str(C[0]) +' & '+ str(C[1]) + ' & '+ str(C[2]) + ' & '+ str(C[3]) + "\\" +"\\")

                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")

                elif len(TOTAL)==5:

                    S= 'Total & '+ str(TOTAL.iloc[0,0]) +'€ & '+ str(TOTAL.iloc[0,5]) + ' & '+ str(TOTAL.iloc[0,6]) + ' & '+ str(TOTAL.iloc[0,3]) + "€  \\" +"\\"

                    _file.writelines(S )
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")

                    S= 'Total mes anterior & '+ str(TOTAL.iloc[1,0]) +'€ & '+ str(TOTAL.iloc[1,5]) + ' & '+ str(TOTAL.iloc[1,6]) + ' & '+ str(TOTAL.iloc[1,3]) +  "€ \\" +"\\"

                    _file.writelines(S )
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")
                    n=3
                    C=[TOTAL.iloc[n,0], TOTAL.iloc[n,5], TOTAL.iloc[n,6], TOTAL.iloc[n,3]]

                    for i in range(len(C)):
                         if (C[i]>0 and i==2) or (C[i]<0 and i!=2):
                             C[i]="\\colorbox{pos}{"+str(C[i])+"}"
                         elif (C[i]<0 and i==2) or (C[i]>0 and i!=2):
                             C[i]="\\colorbox{neg}{"+str(C[i])+"}"
                         else:
                             C[i]=str(C[i])

                    _file.writelines('Comparacion mes anterior & '+ str(C[0]) +' & '+ str(C[1]) + ' & '+ str(C[2]) + ' & '+ str(C[3]) + " \\" +"\\")
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")

                    S= 'Total anio anterior & '+ str(TOTAL.iloc[2,0]) +'€ & '+ str(TOTAL.iloc[2,5]) + ' & '+ str(TOTAL.iloc[2,6]) + ' & '+ str(TOTAL.iloc[2,3]) + "€ \\" +"\\"

                    _file.writelines(S )
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")

                    n=4
                    C=[TOTAL.iloc[n,0],TOTAL.iloc[n,5],TOTAL.iloc[n,6],TOTAL.iloc[n,3]]
                    for i in range(len(C)):
                        if (C[i]>0 and i==2) or (C[i]<0 and i!=2):
                            C[i]="\\colorbox{pos}{"+str(C[i])+"}"
                        elif (C[i]<0 and i==2) or (C[i]>0 and i!=2):
                            C[i]="\\colorbox{neg}{"+str(C[i])+"}"
                        else:
                            C[i]=str(C[i])

                    _file.writelines('Comparacion anio anterior & '+ str(C[0]) +' & '+ str(C[1]) + ' & '+ str(C[2]) + ' & '+ str(C[3]) + "\\" +"\\")
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")

                else:
                    S= 'Total & '+ str(TOTAL.iloc[0,0]) +'€  & '+ str(TOTAL.iloc[0,5]) + ' & '+ str(TOTAL.iloc[0,6]) + ' & '+ str(TOTAL.iloc[0,3]) + "€ \\" +"\\"

                    _file.writelines(S )
                    _file.writelines("\n")
                    _file.writelines("\hline")
                    _file.writelines("\n")
            else:
                #print(line.strip())
                _file.writelines(line)

    output=os.system("pdflatex "+N+".tex")
    assert output==0
    os.remove(N+".aux")
    os.remove(N+".log")
    os.remove(N+".tex")
