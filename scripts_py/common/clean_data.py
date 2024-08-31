import polars as pl
from scripts_py.classes.logger import Logger

logger = Logger(__name__).get_logger()
total_registros_inicial = None


def inicializar_total_registros(df):
    global total_registros_inicial
    total_registros_inicial = df.height
    logger.info(f"Total registros inicializados: {total_registros_inicial}")


def obtener_total_registros():
    global total_registros_inicial
    return total_registros_inicial


def contar_registros_url(df, regex):
    return df.filter(pl.col("url").str.contains(regex)).height


def contar_registros_user_agent(df, regex):
    return df.filter(pl.col("user_agent").str.contains(regex)).height


# Robos y Crawlers
def contar_filtros_robots(df):
    total_registros = obtener_total_registros()

    registros_googlebot = contar_registros_user_agent(df, 'Googlebot')
    registros_baiduspider = contar_registros_user_agent(df, 'Baiduspider')
    registros_agesic_crawler = contar_registros_user_agent(
        df, 'agesic-crawler')

    porcentaje_googlebot = (registros_googlebot / total_registros) * 100
    porcentaje_baiduspider = (registros_baiduspider / total_registros) * 100
    porcentaje_agesic_crawler = (
        registros_agesic_crawler / total_registros
        ) * 100

    logger.info(f"Registros 'Googlebot': {registros_googlebot} ({porcentaje_googlebot:.2f}%)")
    logger.info(f"Registros 'Baiduspider': {registros_baiduspider} ({porcentaje_baiduspider:.2f}%)")
    logger.info(f"Registros 'agesic-crawler': {registros_agesic_crawler} ({porcentaje_agesic_crawler:.2f}%)")


def filtrar_googlebot(df):
    return df.filter(~pl.col("user_agent").str.contains('Googlebot'))


def filtrar_baiduspider(df):
    return df.filter(~pl.col("user_agent").str.contains('Baiduspider'))


def filtrar_agesic_crawler(df):
    return df.filter(~pl.col("user_agent").str.contains('agesic-crawler'))


def filtrar_robots_y_crawlers(df):
    contar_filtros_robots(df)
    df = filtrar_googlebot(df)
    df = filtrar_baiduspider(df)
    df = filtrar_agesic_crawler(df)
    return df


# Servicios de mapas
def contar_filtros_servicios_mapas(df):
    total_registros = obtener_total_registros()

    registros_map_server = contar_registros_url(df, '/wfsPCN1000.cgi')
    registros_wms = contar_registros_url(df, 'SERVICE=WMS')

    porcentaje_map_server = (
        registros_map_server / total_registros
        ) * 100 if total_registros > 0 else 0
    porcentaje_wms = (
        registros_wms / total_registros
        ) * 100 if total_registros > 0 else 0

    logger.info(f"Registros '/wfsPCN1000.cgi': {registros_map_server} ({porcentaje_map_server:.2f}%)")
    logger.info(f"Registros 'SERVICE=WMS': {registros_wms} ({porcentaje_wms:.2f}%)")


def filtrar_map_server(df):
    return df.filter(~pl.col("url").str.contains('/wfsPCN1000.cgi'))


def filtrar_wms(df):
    return df.filter(~pl.col("url").str.contains('SERVICE=WMS'))


def filtrar_solicitudes_servicios_mapas(df):
    contar_filtros_servicios_mapas(df)
    df = filtrar_map_server(df)
    df = filtrar_wms(df)
    return df


# Archivos estáticos
def contar_filtros_archivos_estaticos(df):
    total_registros = obtener_total_registros()

    registros_jcemediabox = contar_registros_url(df,
                                                 '/plugins/system/jcemediabox/'
                                                 )
    registros_css = contar_registros_url(df, r'\.css$')
    registros_js = contar_registros_url(df, r'\.js$')
    registros_png = contar_registros_url(df, r'\.png$')
    registros_jpg = contar_registros_url(df, r'\.jpg$')
    registros_gif = contar_registros_url(df, r'\.gif$')
    registros_favicon = contar_registros_url(df, r'favicon\.ico$')

    # Manejo de casos donde no se encuentran registros
    porcentaje_jcemediabox = ((registros_jcemediabox / total_registros) *
                              100 if registros_jcemediabox else 0)
    porcentaje_css = ((registros_css / total_registros) *
                      100 if registros_css else 0)
    porcentaje_js = ((registros_js / total_registros) *
                     100 if registros_js else 0)
    porcentaje_png = ((registros_png / total_registros) *
                      100 if registros_png else 0)
    porcentaje_jpg = ((registros_jpg / total_registros) *
                      100 if registros_jpg else 0)
    porcentaje_gif = ((registros_gif / total_registros) *
                      100 if registros_gif else 0)
    porcentaje_favicon = ((registros_favicon / total_registros) *
                          100 if registros_favicon else 0)

    logger.info(f"Registros 'jcemediabox': {registros_jcemediabox} ({porcentaje_jcemediabox:.2f}%)")
    logger.info(f"Registros '.css': {registros_css} ({porcentaje_css:.2f}%)")
    logger.info(f"Registros '.js': {registros_js} ({porcentaje_js:.2f}%)")
    logger.info(f"Registros '.png': {registros_png} ({porcentaje_png:.2f}%)")
    logger.info(f"Registros '.jpg': {registros_jpg} ({porcentaje_jpg:.2f}%)")
    logger.info(f"Registros '.gif': {registros_gif} ({porcentaje_gif:.2f}%)")
    logger.info(f"Registros 'favicon.ico': {registros_favicon} ({porcentaje_favicon:.2f}%)")


def filtar_jcemediabox(df):
    return df.filter(~pl.col("url").str
                     .contains('/plugins/system/jcemediabox/'))


def filtrar_css(df):
    regex = r'\.css$'
    return df.filter(~pl.col("url").str.contains(regex))


def filtrar_js(df):
    regex = r'\.js$'
    return df.filter(~pl.col("url").str.contains(regex))


def filtrar_png(df):
    regex = r'\.png$'
    return df.filter(~pl.col("url").str.contains(regex))


def filtrar_jpg(df):
    regex = r'\.jpg$'
    return df.filter(~pl.col("url").str.contains(regex))


def filtrar_gif(df):
    regex = r'\.gif$'
    return df.filter(~pl.col("url").str.contains(regex))


def filtrar_favicon(df):
    regex = r'favicon\.ico$'
    return df.filter(~pl.col("url").str.contains(regex))


def eliminar_peticiones_internas(df):
    return df.filter(~((pl.col("ip") == "127.0.0.1") &
                       (pl.col("request_method") == "OPTIONS") &
                       (pl.col("url") == "*")))


def filtrar_datos(df):
    try:
        contar_filtros_archivos_estaticos(df)
        df = filtrar_robots_y_crawlers(df)
        df = filtrar_solicitudes_servicios_mapas(df)
        df = filtar_jcemediabox(df)
        df = filtrar_css(df)
        df = filtrar_js(df)
        df = filtrar_png(df)
        df = filtrar_jpg(df)
        df = filtrar_gif(df)
        df = filtrar_favicon(df)
        df = eliminar_peticiones_internas(df)
    except Exception as e:
        logger.error(f"Error al limpiar los datos: {e}")
        raise
    return df
