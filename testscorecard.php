<?php
/**
 * Este archivo es parte del entorno de procesamiento Simetrical Server/SimServer
 *
 * Este software es propiedad de Simetrical, S.A. de C.V.,
 * no es un software libre y no puede ser redistribuído y/o
 * modificado bajo ningún término que no sea expresamente
 * autorizado por el propietario.
 *
 * @copyright Copyright &copy; 2020 Simetrical, S.A. de C.V.
 * @link http://www.simetrical.com
 *
 * @filesource
 *
 */
include_once dirname(__FILE__)."/Libraries/SimApp/constantes_sistema.php";
include_once dirname(__FILE__)."/constantes_aplicacion_testing.php";


if (SO == "WINDOWS") {
	define("RAIZ_S", "D:");
	define("RAIZ_E", "D:");
	define("SMig", "");
	define("SM", "/SM");

	define("PROCESS_WAIT_NAME", "/respaldo/convertidos/deldia");
	define("RAIZ_SMIG", RAIZ_S.SMig.SM);
}

if (SO == "LINUX")
	define("RAIZ_S", "/tmp");

if (!defined("CLASE_ABSTRACTA"))
	define("CLASE_ABSTRACTA", 1);

include_once dirname(__FILE__)."/scorecard.php";

/**
 * Construye las bases de datos para presentar datos en un tablero de control
 *
 *
 * @author Noe Castillo Sosa <ncastillo@simetrical.com.mx>
 * @version 1.0
 *
 */
class TestScorecard extends Scorecard {

	/**
	 * Version del programa que sera impresa en la leyenda
	 *
	 * @var string
	 */
	protected $version = "1.0";

	protected $valida_cliente = null;

	/**
	 * Numero de cliente a procesar
	 *
	 * @var int
	 */
	protected $client = null;

	protected $branch = null;

	/**
	 * Define la fecha de analisis
	 *
	 * @var int
	 */
	// protected $hoy = null;

	// protected $ayer = null;

	protected $dsn_remoto = null;

	protected $proceso = "*";

	protected $procesos = null;

	protected $paquetes = array();

	protected $paquete = null;

	protected $dia = null;

	protected $fecha_paquete = null;

	protected $sincronizar = null;

	protected $dump = null;

	protected $descargar = null;

	protected $plantas = null;

	protected $respaldar = null;

	protected $terminar = null;

	protected $identificado = null;

	protected $total_paquetes = null;

	protected $meses_proceso = null;

	protected $meses_detalles = null;

	protected $industria = null;

	protected $pais = null;

	protected $sucursales = null;

	protected $marcas = null;

	protected $divisiones = array();

	protected $ano_final = null;

	protected $mes_final = null;

	protected $ignorar = null;

	protected $historico = null;

	protected $desarrollo = true;

	protected $limpia_log = false;

	public static function singleton($clase = null) {
		return parent::singleton($clase ? $clase : __CLASS__);
	}

	public function __construct($nombre = null,
		$descripcion = "Modo : Test - Construye las bases de datos para presentar datos en un tablero de control",
		$argv = null) {

		$this->dsn_remoto = "mysql://".USUARIO_DB.":".CONTRASENA_DB."@".DATABASE_SERVER.":".PUERTO_DB;

		parent::__construct($nombre, $descripcion, $argv);

		$this->valida_cliente = in_array("valida_cliente", $GLOBALS["argv"], true);
		if ($this->valida_cliente)
			if (!($this->client = (int)$this->client))
				$this->error("No se ha definido un cliente");

		// $proceso = explode(",", $this->proceso);
		// $this->proceso = $proceso[0];

		if (is_null($this->procesos))
			$this->procesos = $this->proceso;

		$this->sincronizar 	= !in_array("no_sincronizar", $GLOBALS["argv"], true);
		$this->dump 		= !in_array("no_dump", $GLOBALS["argv"], true);
		$this->descargar 	= !in_array("no_descargar", $GLOBALS["argv"], true);
		$this->transformar 	= !in_array("no_transformar", $GLOBALS["argv"], true);
		$this->plantas 		= !in_array("no_plantas", $GLOBALS["argv"], true);
		$this->postprocesos = !in_array("no_postprocesos", $GLOBALS["argv"], true);
		$this->transferir 	= !in_array("no_transferir", $GLOBALS["argv"], true);
		$this->respaldar 	= !in_array("no_respaldar", $GLOBALS["argv"], true);
		$this->terminar 	= !in_array("no_terminar", $GLOBALS["argv"], true);
		$this->log 			= in_array("no_log", $GLOBALS["argv"], true);
		$this->ignorar 		= in_array("ignorar", $GLOBALS["argv"], true);
		$this->historico 	= in_array("historico", $GLOBALS["argv"], true);

		if ($this->ignorar)
			$this->consulta("SET SESSION sql_mode=''");

		$this->desarrollo 	= true;
		$this->sincCliente 	= 1;
		$this->sincMfr		= 1;
		$this->sincNlac 	= 1;

	}


	protected function limpiaMaquinaVirtual($mantener = false, $terminando = false) {
		$this->depurar(__METHOD__."($mantener, $terminando)");

		$dummy = $this->listaArchivos(".zip" , SW_WORKING);

		if (!empty($dummy)) {
			$this->mueveArchivo(SW_WORKING."/".$dummy[0] , SW_TODAY."/".$dummy[0]);
		}

		$this->eliminaArchivo(TEMP."/sincronizado.txt", null);
		parent::limpiaEspacio();

		$directorio = RAIZ_S.SC."/BATCH/Migracion";

		$archivos = array (
			'00-SYNC.bat',
			'30-CONSOL.bat',
			'70-SC.bat',
			'80-TRF.bat',
			'DESCARGA-PAQUETES.bat',
			'LIMPIAMIG.bat',
			'RESPALDA.bat');

		foreach ($archivos as $archivo) {
			$this->copiaArchivos($directorio."/".$archivo , RAIZ_S.SW."/");
		}

		$this->mensaje();
		return $this;
	}

	protected function copiaDump_client($origen , $destino, $basedatos) {
		$this->depurar(__METHOD__);

		$directorioDestino	= $destino."/".$basedatos;
	 	$directorioOrigen 	= $origen."/".$basedatos;
	 	$tablasOrigen		= $this->listaArchivos(".sql.dump",$directorioOrigen);

		if (!$this->existeDirectorio($destino."/".$basedatos))
			$this->creaDirectorio($destino."/".$basedatos);

		$this->mensaje("   Copiando archivos de '".$basedatos."'...");

		foreach ($tablasOrigen as $tabla) {
			$this->copiaArchivos($directorioOrigen."/".$tabla , $directorioDestino."/");
			echo ".";
		}

		$this->mensaje();
		return $this;
	}

	protected function copiaDump_mfr($origen , $destino, $basedatos) {
		$this->depurar(__METHOD__);

		$directorioDestino	= $destino."/mfr/".$basedatos;
	 	$directorioOrigen 	= $origen."/mfr_".$basedatos;
	 	$tablasOrigen		= $this->listaArchivos(".sql.dump",$directorioOrigen);

		if (!$this->existeDirectorio($destino."/mfr/".$basedatos))
			$this->creaDirectorio($destino."/mfr/".$basedatos);

		$this->mensaje("   Copiando archivos de '".$basedatos."'...");

		foreach ($tablasOrigen as $tabla) {
			$this->copiaArchivos($directorioOrigen."/".$tabla , $directorioDestino."/");
			echo ".";
		}

		$this->mensaje();
		return $this;
	}

	protected function copiaDumps() {
		$this->depurar(__METHOD__);

		$this->identificaCliente();
		// $this->validasincronizacion();
		$this->mensaje();

		$this->mensaje("Copiando Dumps de remoto a local ".RAIZ_P." => ".DB." \n");

		if ($this->sincCliente > 0) {
			$this->copiaDump_client(RAIZ_P, RAIZ_S.DB,$this->client);
		} else {
			$this->mensaje("      ALERTA: El cliente  ".$this->client. " ya fue sincronizado el dia ".date("Y-m-d").". Si desea forzar la sincronizacion elimine el archivo  ".DB."/".$this->client.".txt");
			$this->mensaje();
		}

	/*
		foreach ($this->marcas as $marca => $parametros) {
			$mfr = strtolower($parametros["Nombre"]);
			$mfr = str_replace(" ", "_", strtolower($mfr));
				if ($this->sincMfr > 0) {
					$this
						->copiaDump_mfr(RAIZ_P, RAIZ_S.DB,$mfr);
				} else {
					$this->mensaje("      ALERTA: La planta ".strtoupper($mfr)." ya fue sincronizado el dia ".date("Y-m-d").". Si desea forzar la sincronizacion elimine el archivo  ".DB."/$mfr.txt");
					$this->mensaje();
				}
		}

		if ($mfr == "ncl" or $mfr == "nar" or $mfr == "npe" or $mfr == "npa") {
			if ($this->sincNlac > 0) {
				$this
					->copiaDump_mfr(RAIZ_P, RAIZ_S.DB,"nlac");
			} else {
				$this->mensaje("      ALERTA: La planta NLAC ya fue sincronizado el dia ".date("Y-m-d").". Si desea forzar la sincronizacion elimine el archivo  ".DB."/nlac.txt");
				$this->mensaje();
			}
		}


		$basedatos = array ('indice','mfr','crm_simetrical');
		foreach ($basedatos as $bd) {
			$this
				->copiaDump_(RAIZ_E, RAIZ_DUMP,$bd);
		}
		*/
		$this->mensaje();
		return $this;
	}

	protected function descargaPaquetes() {
		$this->depurar(__METHOD__);

		$existePaquete = count($this->listaArchivos('.zip' , SW_WAIT));
		$existePaquete = ($existePaquete > 0) ? true : false ;

		if (!$this->client)
			$this->identificaCliente();

		if ($this->descargar) {
			if ($this->client) {
				$this->mensaje("Descargando paquetes remotos almacenados para el cliente '{$this->client}'...");

				if (count($remotos = $this->listaArchivosRemotos(URI_FTP_PROCESS."respaldo/convertidos/deldia"))) {
					$paquetes = array();

					foreach ($remotos as $paquete) {
						if ((int)substr($paquete, 0, 4) == $this->client) {
							$this->mensaje("   Descargando '$paquete'...");

							$paquetes[] = $paquete;

							$this->descargaArchivoFTP("respaldo/convertidos/deldia"."/".$paquete,
								URI_FTP_PROCESS, SW_WAIT."/".$paquete, false);
						}
					}

					if (count($remotos = $paquetes))
						$this
							->depurar(print_r($remotos, true))
							->mensaje();

					$this->mensaje("   ".count($remotos)." paquetes descargados");
				}

				$this->mensaje();
			}
		}

		return $this;
	}

	protected function procesaPaquetes() {
		$this->depurar(__METHOD__);

		if ($this->client) {
			do {
				$this->mensaje("Listando paquetes por procesar...");

				$this->paquetes = array();

				if (count($paquetes = $this->listaArchivos(".zip", SW_WAIT))) {
					foreach ($paquetes as $paquete)
						if ((int)substr($paquete, 0, 4) == $this->client)
							$this->paquetes[] = $paquete;

					$this
						->depurar(print_r($this->paquetes, true))
						->mensaje("   ".count($this->paquetes)." paquetes encontrados ".date("H:i:s"))
						->mensaje();

					foreach ($this->paquetes as $paquete) {
						$this
							->limpiaEspacio(false, true)
							->mueveArchivo(SW_WAIT."/".$paquete, SW_WORKING."/".$paquete);

						$this->paquete = $paquete;

						$this->procesaPaquete();
					}
				}

			} while (count($paquetes));
		}

		$this->mensaje();

		return $this;
	}

	protected function procesaPaquete() {
		$this->depurar(__METHOD__);

		$this
			->identificaCliente()
			->mensaje("   Procesando paquete '{$this->paquete}'...");

		$this->descomprimePaquete();

		$this
			->importaTablas()
			->consolidaTablas()
			->formateaMAC()
			->actualizaMAC()
			->actualizaTablas();

		// $this->mueveArchivo(SW_WORKING."/".$this->paquete, SW_TODAY."/".$this->paquete);

		$this->mensaje();

		return $this;
	}

	protected function importaTablas() {
		$this->depurar(__METHOD__);

		$this->identificaCliente();

		if ($this->client) {
			$this->mensaje("      Importando las tablas...");

			if ($dsn = $this->dsn)
				$dsn = "dsn=$dsn";

			$php = "php";
			if (SO == "WINDOWS")
				$php = "%php52%";

			$depurar = null;
			if ($this->depurar)
				$depurar = "depurar";

			$historico = null;
			if ($this->historico)
				$historico = "historico";

			$consulta = "DROP DATABASE IF EXISTS ".PREFIJO_SND.$this->client;
			$this->consulta($consulta);

			$archivos_dbs = $this->listaArchivos('.db', SW_SANDBOX.'/');
			$archivos_txt = $this->listaArchivos('.txt', SW_SANDBOX.'/');
			$archivosConfig = array('mac', 'tablas', 'chart', 'galib', 'types');

			foreach ($archivosConfig as $archivo) {
				$pattern = "/^".strtoupper($archivo).".DB(\d*).db/";
				if ($db = preg_grep($pattern, $archivos_dbs)) {
					$this->eliminaArchivo(implode(',', $db), SW_SANDBOX . '/');
				}
				$this->eliminaArchivo(strtoupper($archivo).".DB", SW_SANDBOX . '/');
			}

			if (in_array('dump.txt', $archivos_txt)) {
				$comando = "$php -f ".dirname(__FILE__)."/dump2mysql.php -- $dsn directorio=".SW_SANDBOX." basedatos=".PREFIJO_SND.$this->client." no_temporal $depurar";
			} elseif (in_array('csv.txt', $archivos_txt)) {
				if (SO == "WINDOWS")
					$php = "%php53%";

				$comando = "$php -f ".dirname(__FILE__)."/csv2mysql.php -- $dsn directorio=".SW_SANDBOX." basedatos=".PREFIJO_SND.$this->client." limpiar $depurar $historico";
			} else {
				$comando = "$php -f ".dirname(__FILE__)."/pdx2mysql.php -- $dsn directorio=".SW_SANDBOX." basedatos=".PREFIJO_SND.$this->client." limpiar $depurar";
			}
			$this->ejecutaComando($comando, true, true);

			if (in_array('csv.txt', $archivos_txt) || in_array('dump.txt', $archivos_txt)) {
				$this->deDuplic();
			}
		}

		return $this;
	}

	protected function deDuplic()
	{
		$this->depurar(__METHOD__);

		$this->mensaje("Aplicando proceso deduplica...");
		
		$baseDatosAnterior = $this->baseDatosActual();
		if ($this->existeBaseDatos(PREFIJO_SND.$this->client)) {
			$this->usarBaseDatos(PREFIJO_SND.$this->client, false);

			$tablas = array("%de%", "%vt%", "%com%", "refser%", "sertect%", "refoep%", "refmos%", "refcom%");
			$campos = array("Total$", "Venta$", "VentaTotal$", "Saldo$", "Costo$", "Valor$", "HorasFacturadas");

			foreach ($tablas as $tabla) {
				$tablasEncontradas = array();
				$resultado = $this->consulta("SHOW TABLES LIKE '{$tabla}'");
	
				if($resultado) {
					while ($fila = $resultado->fetch_row()) {
						$tablasEncontradas[] = $fila[0];
					}
	
					if (@count($tablasEncontradas) > 0) {
						foreach($tablasEncontradas as $tablaEncontrada) {
							if (!$this->existeCampo('IdRegistro', $tablaEncontrada)) {
								continue;
							}
	
							$this->mensaje("    " . $tablaEncontrada . "...");
							$consulta = "UPDATE $tablaEncontrada SET ";
							$contador = 0;
							foreach ($campos as $campo) {
								if ($this->existeCampo($campo, $tablaEncontrada)) {
									$consulta .= " {$campo} = {$campo} + (IdRegistro * .00000001),";
									$contador = $contador + 1;
								}
							}
							if ($contador > 0) {
								$this->consulta(trim($consulta, ","));
								$this->mensaje("      Registros actualizados " .$this->filas_afectadas);
							}
						}
					}
				}
			}

			// Eliminando idRegistro
			$tablas = $this->listaTablas();
			if(@count($tablas)) {
				foreach (array_keys($tablas) as $tabla) {
					if ($this->existeCampo('IdRegistro', $tabla)) {
						
						$this->mensaje("    Eliminado la columna IdRegistro de la tabla " . $tabla);
						$consulta  = "ALTER TABLE {$tabla} DROP COLUMN IdRegistro";
						$this->consulta($consulta);
					}
				}
			}
		}

		$this->usarBaseDatos($baseDatosAnterior);

		return $this;
	}

	protected function validaTabla() {
		$this->depurar(__METHOD__);

		if (!$this->existeTabla(PREFIJO_SIM.$this->client. '.tablas')){
			$this->mensaje("\n   El cliente no tiene configurado 'tablas'...");
			$this->clonaTabla("indice.auto-tbl", PREFIJO_SIM.$this->client.".tablas");
			$this->mensaje("      Obteniendo 'tablas' de 'auto_tbl'...\n");

			if ($this->existeArchivo($this->client."_auto-table.txt",RAIZ_S.DB."/"))
				$this->eliminaArchivo($this->client."_auto-table.txt",RAIZ_S.DB."/");

				$this->escribeArchivo(RAIZ_S.DB."/".$this->client."_auto-table.txt", null, date("Y-m-d"));

		}

		$this->mensaje();

		return $this;
	}

	protected function procesaPaqueteMonarc() {
		$this->depurar(__METHOD__);

		$this
			->identificaCliente();

		$this->validaTabla();

		$this
			->importaTablas()
			->consolidaTablas()
			->formateaMAC()
			->actualizaMAC()
			->actualizaTablas();

		// $this->mueveArchivo(SW_WORKING."/".$this->paquete, SW_TODAY."/".$this->paquete);
		//$this->limpiaEspacio();

		$this->mensaje();

		return $this;
	}

	protected function importa() {
		$this->depurar(__METHOD__);

		$this->importaTablas();

		return $this;
	}

	protected function consolida() {
		$this->depurar(__METHOD__);

		parent::procesoExterno('antes','consolida','1');
		parent::consolidaTablas();
		parent::procesoExterno('despues','consolida','1');

		return $this;
	}

	protected function mac() {
		$this->depurar(__METHOD__);

		parent::procesoExterno('antes','mac','1');
		parent::formateaMAC();
		parent::actualizaMAC();
		parent::procesoExterno('despues','mac','1');

		return $this;
	}

	protected function tablas() {
		$this->depurar(__METHOD__);

		parent::procesoExterno('antes','tablas','1');
		parent::actualizaTablas();
		parent::procesoExterno('despues','tablas','1');

		return $this;
	}

	protected function respalda($tablas = null, $directorio = null) {
		$this->depurar(__METHOD__);

		if (!$this->client)
			$this->identificaCliente();

		$this->validaTablas();

		$tablas = implode(",", $this->tablasSinc) ;

		$directorio = RAIZ_J."/".$this->client;

		if ($this->existeArchivo($this->client."_auto-table.txt", RAIZ_S.DB."/")){
			$this
				->eliminaTabla(PREFIJO_SIM.$this->client.".tablas")
				->mensaje("El cliente importo la tabla 'tablas' de auto-tbl...");
		}

		parent::respalda($tablas, $directorio);

		$this->mensaje();
		return $this;
	}

	protected function transfiereSQLite($demo = null, $dataserver = null , $test = false) {
		$this->depurar(__METHOD__);

		$server = "Testscorecard.simetrical.net";

		parent::transfiereSQLite(null, $server, true);

		$this->mensaje();
		return $this;
	}

	protected function sincronizaData($bd = null , $DATABASE_SERVER = null) {
		$this->depurar(__METHOD__);

		$server = ($this->existeArchivo("UsrServer.txt", RAIZ_S)) ? $server = "datatest.simetrical.internal" : "datatest.simetrical.net" ;

		if ($this->existeArchivo('sinc-rap.txt', RAIZ_DUMP."/")) {
			$this
				->validaTablas()
				->sincronizaRapida(null, $server , $this->tablasSinc );
		} else {
			parent::sincronizaData(null, $server);

		}

		$this->mensaje();
		return $this;
	}

	protected function descargaDumps() {
		$this->depurar(__METHOD__);

		$this->mensaje();
		return $this;
	}

	protected function indicadoresContables() {
		$this->depurar(__METHOD__);

		$this->mensaje("Test - Calcula Contables Tablero Modo Testing...");

		parent::indicadoresContables();

		$this->mensaje();
		return $this;
	}

	protected function exporta() {
		$this->depurar(__METHOD__);

		$this->mensaje("Test - Exportando Tablero Modo Testing...");

		parent::exporta();

		return $this;
	}

	protected function tablero() {
		$this->depurar(__METHOD__);

		$this->mensaje("Test - Ejecutando Tablero Modo Testing...");

		parent::tablero();

		return $this;
	}

	protected function sincronizaCliente() {
		$this->depurar(__METHOD__);

		$this->mensaje("Test - Ejecutando Tablero Modo Testing...");

		$this->eliminaArchivo("sincronizado.txt" , TEMP."/");

		parent::sincronizaCliente();

		return $this;
	}

	protected function sincronizaMfr() {
		$this->depurar(__METHOD__);

		$this
			->mensaje()
			->mensaje("Hora Inicia : ".date("H:i:s"))
			->mensaje();

		$this->mensaje("Test - Sincronizando Mfr con contrato...");
		$this->decodificaDSN($this->dsn,$servidor, $usuario, $contrasena, $basedatos, $puerto);
		$mysqldump 		= (SO == "WINDOWS") ? '%mysqldump%' : 'mysqldump';
		$mysql 			= (SO == "WINDOWS") ? '%mysql%' : 'mysql';

		if ($this->sincronizar) {

			$this->mensaje("Obtenemos todas las marcas de Simetrical...");

			$consulta = "SELECT m.MadeId, m.Marca
						FROM crm_simetrical.client_made AS c
						LEFT JOIN indice.marcas AS m ON (IF(c.Made ='Nissan', 1999, (c.idMarca+8000)) = m.MadeId)
						WHERE c.Contrato = 1
						AND c.DBU = 1";

			if ($resultado = $this->consulta($consulta)) {
				while ($fila = $resultado->fetch_assoc()) {
					$this->marcas[(int)$fila["MadeId"]]["Nombre"] = trim($fila["Marca"]);
					$this->marcas[(int)$fila["MadeId"]]["Fabricante"] = strtolower(str_replace(" ", "_", $fila["Marca"]));
					$this->marcas[(int)$fila["MadeId"]]["NombreCorto"] = strtoupper(str_replace(" ", "", substr($fila["Marca"], 0, 8)));
				}
			}

			$this->mensaje("   Marcas encontradas con contrato...");
			foreach ($this->marcas as $marca_id => $parametros) {
				$this->mensaje("      '{$parametros["Nombre"]}'...");
			}

			foreach ($this->marcas as $marca_id => $parametros) {

					$this->mensaje();

					$this->mensaje("Sincronizando '{$parametros["Nombre"]}'...");

					$fabricante 	= str_replace(" ", "_", strtolower($parametros["Nombre"]));
					$fabricante 	= str_replace("-", "_", strtolower($fabricante));
					$plantas		= array($fabricante);
					$dumps 			= parent::ObtieneDumps($plantas, true);
					$dumps 			= array_keys($dumps);

					if ($this->continua == 1 ) {
						$this->mensaje("Descargando archivos...");
						$comando = "$mysql -h $servidor -P $puerto -u $usuario -p$contrasena -e \"DROP DATABASE IF EXISTS mfr_$fabricante; CREATE DATABASE mfr_$fabricante\"";

						if($this->ejecutaComando($comando))
							$this->mensaje("Limpiando base de datos...");

						$contador = 0;
						foreach ($dumps as $dump) {
							if ( ($fabricante == 'infiniti') or ($fabricante == 'fco') ) {
								if ($contador == 60) {
									$this->mensaje("\nBalanceando importancion...");
									sleep(60);
									$contador = 0;
								}
							}

							$cmd = "$mysql mfr_$fabricante -h $servidor -P $puerto -u $usuario -p$contrasena --force < $dump";
							if (SO == 'WINDOWS') {
								$cmd = 'start "" /B '. $cmd;
								pclose(popen($cmd, "r"));
							} else {
								$cmd .= " >> ".TEMP."/sincronizaDump.log & ";
								$this->ejecutaComando($cmd , false);
							}

							echo ".";
							$contador++;
						}

						$procesos_corriendo = 0;
						$cont_procesos_cero = 0;

						$this->mensaje("\nSincronizando tablas a base de datos local  mfr_$fabricante...");
						do {
							$consulta = " SELECT * FROM INFORMATION_SCHEMA.PROCESSLIST WHERE DB = 'mfr_$fabricante' AND INFO LIKE 'INSERT INTO%'";
							if ($resultado = $this->consulta($consulta)) {
								if (mysqli_num_rows($resultado) == 0) {
									$cont_procesos_cero++;
									$procesos_corriendo = 0;
								} else {
									$procesos_corriendo = mysqli_num_rows($resultado);
									$cont_procesos_cero = 0;
								}
							}

							$this->depurar("Procesos corriendo: $procesos_corriendo");
							$this->depurar("Contador cero procesos : $cont_procesos_cero");

							sleep(1);
							echo ".";

						} while ($cont_procesos_cero < 3);

					}

					$dir = (SO == 'WINDOWS') ? SD_MFR ."/" : RAIZ_E ."/mfr_";
					parent::ejecutaValidacionSinc($dir  . $fabricante, "mfr_" . $fabricante);
					$this->mensaje();

			}

		}

		$this
			->mensaje()
			->mensaje("Hora Finaliza : ".date("H:i:s"))
			->mensaje();

		return $this;
	}

	protected function limpiaMaster() {
		$this->depurar(__METHOD__);

		$this->mensaje("Limpiando espacio de trabajo");
		$this->limpiaMaquinaVirtual();


		$this->mensaje();

		$this->mensaje("Limpiando Unidad ".RAIZ_S.DB);
		$this->limpiaDirectorio(null, RAIZ_S.DB , true , 0, null);

		;

		$consulta = "
		SELECT DISTINCT schema_name
		FROM information_schema.SCHEMATA
		WHERE schema_name LIKE 'mfr_%'
			OR schema_name LIKE 'sim_%'
			OR schema_name LIKE 'sc_%'
			OR schema_name LIKE 'snd_%'";

		$resultado = $this->consulta($consulta);

		foreach ($resultado as $basedatos ) {
			foreach ($basedatos as  $basedato) {
				$this->mensaje("   Eliminando base de datos '$basedato'...");
				$this->consulta("DROP DATABASE `$basedato`");
			}

		}

		if ($this->limpia_log) {
			$this->mensaje("");
			$this->mensaje("Deteniendo Servicio Mysql...");
			$this->mensaje("Limpiando base de datos local...");

			$estado = 0;
			$comando = 'net stop mysql55';
			if ($this->ejecutaComando($comando)){
				$this->mensaje("   Servicio detenido correctamente...");
				$estado = 1;
			}

			$this->mensaje();
			$directorio = 'C:\ProgramData\MySQL\MySQL Server 5.5\data\\';
			$archivos = array ('ibdata1','ib_logfile0','ib_logfile1');

			if ($estado == 1) {
				foreach ($archivos as $archivo) {
					if ($this->existeArchivo($archivo , $directorio)) {
						if ($this->eliminaArchivo($archivo, $directorio)) {
							$this->mensaje("   Eliminando $archivo...");
						}
					}
				}
			} else {
				$this->mensaje("   El servicio MYSQL no ha sido detenido...");
			}

			$this->mensaje();
			$this->mensaje("Iniciando Servicio Mysql...");
			if ($estado == 1) {
				$comando = 'net start mysql55';
				if ($this->ejecutaComando($comando)){
					$this->mensaje("   Servicio Iniciado correctamente...");
					$estado = 1;
				}
			} else {
				$this->mensaje("   El servicio MYSQL no ha sido detenido...");
			}

		}

		$this->mensaje();
		return $this;
	}

	protected function copiadummy() {
		$this->depurar(__METHOD__);

		$this->mensaje("Copiando 2-Workng");

		$directorioDestino	= SW_WORKING;
	 	$directorioOrigen 	= "S:\SW\\2-Workng\\";
	 	$tablasOrigen		= $this->listaArchivos(".zip",$directorioOrigen);

		$this->mensaje("   Copiando archivos de 2-Workng...");

		foreach ($tablasOrigen as $tabla) {
			$this->copiaArchivos($directorioOrigen."/".$tabla , $directorioDestino."/");
			echo ".";
		}

		$this->mensaje();



		return $this;
	}

	protected function copiasnd() {
		$this->depurar(__METHOD__);

		$this->mensaje("Copiando 3-Sandbx");

		$directorioDestino	= SW_SANDBOX;
	 	$directorioOrigen 	= "S:\SW\\3-Sandbx\\";
	 	$tablasOrigen		= $this->listaArchivos(".DB",$directorioOrigen);

		$this->mensaje("   Copiando archivos de 3-Sandbx...");

		foreach ($tablasOrigen as $tabla) {
			$this->copiaArchivos($directorioOrigen."/".$tabla , $directorioDestino."/");
			echo ".";
		}

		$this->mensaje();



		return $this;
	}

	protected function validasincronizacion() {
		$this->depurar(__METHOD__);

		$this->mensaje("Validando Sincronizacion...");

		$this->identificaCliente();

		foreach ($this->marcas as $marca => $parametros) {
			$mfr = strtolower($parametros["Nombre"]);
		}

		if ($this->existeArchivo($this->client.".txt",DB."/")) {
			$fp = fopen(DB."/".$this->client.".txt", "r");
			while (!feof($fp)){
			    $fechaClient = fgets($fp);
			}
			$this->sincCliente 	= ($fechaClient == date("Y-m-d")) 	? 0 : 1 ;
		} else {
			$this->sincCliente = 1;
		}

		if ($this->existeArchivo("$mfr.txt",DB."/")) {
			$fp = fopen(DB."/$mfr.txt", "r");
			while (!feof($fp)){
			    $fechaMfr = fgets($fp);
			}
			$this->sincMfr 		= ($fechaMfr == date("Y-m-d")) 		? 0 : 1 ;
		} else {
			$this->sincMfr = 1;
		}

		if ($mfr == "ncl" || $mfr == "nar" || $mfr == "npe" || $mfr == "npa") {
			if ($this->existeArchivo("nlac.txt",DB."/")) {
				$fp = fopen(DB."/nlac.txt", "r");
				while (!feof($fp)){
				    $fechaNlac = fgets($fp);
				}
				$this->sincNlac 		= ($fechaNlac == date("Y-m-d")) 		? 0 : 1 ;
			} else {
				$this->sincNlac = 1;
			}
		}

		$this->depurar($this->sincCliente);
		$this->depurar($this->sincMfr);
		$this->depurar($this->sincNlac);

		return $this;
	}

	protected function validaTablas($respalda = false) {
		$this->depurar(__METHOD__);

		$this->mensaje("Test - Validando Tablas a sincronizar...");

		$this->identificaCliente();

		$this->usarBaseDatos(PREFIJO_SIM.$this->client,true);

		$consulta = "SELECT DISTINCT LOWER(NAME) FROM tablas";
		$resultado = $this->consulta($consulta);

		$tablasbd = array();
		foreach ($resultado as $keyId => $tablas) {
			foreach ($tablas as $keyId => $tabla) {
				array_push($tablasbd, $tabla);
			}
		}

		$tabla = null;
		if (!$this->existeArchivo($this->client."_auto-table.txt" , RAIZ_S . DB . "/"))
			$tabla = 'tablas,';

		$tablasConfig = array(
            'chart',
            'galib',
            'types',
            'gestion',
            'docs',
            'galabels',
            'mac',
            'obejtivo',
            $tabla
        );

		$this->tablasSinc =	array_merge($tablasbd, $tablasConfig);

		return $this;
	}

	protected function principal() {
		$this->depurar(__METHOD__);

		$procesos = explode(",", $this->procesos);
		$this->depurar(print_r($procesos, true));

		foreach ($procesos as $proceso)
			switch (strtolower($proceso)) {
				case "*":
					$this
						->limpiaEspacio()
						->sincronizaCliente()
						->descargaPaquetes()
						->procesaPaquetes()
						->indicadoresContables()
						->tablero()
						->exporta()
						->sincronizaData()
						->ejecutaPostTablero()
						->transfiere()
						->respalda();

					break;
				case "limpia":
					$this->limpiaEspacio();

					break;
				case "copiadumps":
					$this->copiaDumps();

					break;
				case "sincroniza":
					$this->sincronizaCliente();

					break;
				case "descarga":
					$this->descargaPaquetes();

					break;
				case "procesa":
					$this->procesaPaquetes();

					break;
				case "procesa2":
					$this->procesaPaqueteMonarc();

					break;
				case "contables":
					$this->indicadoresContables();

					break;
				case "tablero":
					$this->tablero();

					break;
				case "exporta":
					$this->exporta();

					break;
				case "postprocesos":
					$this
						->sincronizaData()
						->ejecutaPostTablero();

					break;
				case "transfiere":
					$this->transfiereSQLite();

					break;
				case "respalda":
					$this->respalda();

					break;
				case "respaldaconfig":
					$this->respalda("galib,tablas,types,chart");

					break;
				case "respaldatablas":
					$this->respalda("tablas");

					break;
				case "sincronizadata":
					$this->sincronizaData();

					break;
				case "descargadumps":
					$this->descargaDumps();

					break;
				case "validasinc":
					$this->validasincronizacion();

					break;
				case "validatablas":
					$this->validaTablas();

					break;
				case "limpiamig":
					$this->limpiaMaquinaVirtual();

					break;
				case "importa":
					$this->importaTablas();

					break;
				case "consolida":
					$this->consolidaTablas();

					break;
				case "mac":
					$this->mac();
					break;
				case "tablas":
					$this->actualizaTablas();

					break;
				case "detalles":
					$this->creaDetalles();

					break;
				case "copiadummy":
					$this->copiadummy();

					break;
				case "copiasnd":
					$this->copiasnd();

					break;
				case "limpiamaster":
					$this->limpiamaster();

					break;
				case "sincmfr":
					$this->sincronizaMfr();

					break;
				default :
					$this->error("No existe un controlador para el proceso '$proceso'");
			}


		return $this;
	}
}

// if (!CLASE_ABSTRACTA)
	TestScorecard::singleton()->ejecutar();
?>