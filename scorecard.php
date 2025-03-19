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

if (SO == "LINUX")
	define("RAIZ_S", "/tmp");

if (SO == "WINDOWS")
	if (!defined("SMig"))
		define("SMig", "");

if (!defined("CLASE_ABSTRACTA"))
	define("CLASE_ABSTRACTA", 0);

define("DATABASE_SERVER", "database.simetrical.internal");
define("PREFIJO_SIM", "sim_");

include_once dirname(__FILE__)."/Libraries/SimApp/constantes_aplicacion.php";
include_once dirname(__FILE__)."/Libraries/SimApp/aplicacion.php";

/**
 * Construye las bases de datos para presentar datos en un tablero de control
 *
 * ToDo: Datos de Parametros (PARAM)
 * Todo: Agrega Indicador de CDO (I999, I998)
 * ToDo: Calcula Indicadores de Fin de Mes (No hay configurados)
 * ToDo: Calcula Indicadores Mixtos //2020-11-17 Ncastillo
 *
 * @author Edgar Joel Rodríguez Ávalos <erodriguez@simetrical.com>
 * @version 1.0
 *
 */
class Scorecard extends Aplicacion {

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
	protected $hoy = null;

	protected $ayer = null;

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

	protected $esNlac = null;

	/**
	 * Define si se guardar el log en una base de datos
	 *
	 * @var int
	 */
	protected $log = null;

	protected $desarrollo = null;

	public static function singleton($clase = null) {
		return parent::singleton($clase ? $clase : __CLASS__);
	}

	public function __construct($nombre = null,
		$descripcion = "Construye las bases de datos para presentar datos en un tablero de control",
		$argv = null) {

		if (!isset($this->hoy)) {
			$this->hoy = "today";
		}
		$this->dsn_remoto = "mysql://".USUARIO_DB.":".CONTRASENA_DB."@".DATABASE_SERVER.":".PUERTO_DB;

		parent::__construct($nombre, $descripcion, $argv);

		$this->valida_cliente = in_array("valida_cliente", $GLOBALS["argv"], true);
		if ($this->valida_cliente)
			if (!($this->client = (int)$this->client))
				$this->error("No se ha definido un cliente");

		//if (SERVIDOR == "prdserver")
			//$this->archivo_registro = LOGS."/Scorecard/{$this->client}.err";

		$this->hoy = strtotime($this->hoy);
		$this->ayer = strtotime("-1 day", $this->hoy);

		$proceso = explode(",", $this->proceso);
		$this->proceso = $proceso[0];

		if (is_null($this->procesos))
			$this->procesos = $this->proceso;

		$this->sincronizar = !in_array("no_sincronizar", $GLOBALS["argv"], true);
		$this->dump = !in_array("no_dump", $GLOBALS["argv"], true);
		$this->descargar = !in_array("no_descargar", $GLOBALS["argv"], true);
		$this->transformar = !in_array("no_transformar", $GLOBALS["argv"], true);
		$this->plantas = !in_array("no_plantas", $GLOBALS["argv"], true);
		$this->postprocesos = !in_array("no_postprocesos", $GLOBALS["argv"], true);
		$this->transferir = !in_array("no_transferir", $GLOBALS["argv"], true);
		$this->respaldar = !in_array("no_respaldar", $GLOBALS["argv"], true);
		$this->terminar = !in_array("no_terminar", $GLOBALS["argv"], true);
		$this->log = in_array("no_log", $GLOBALS["argv"], true);
		$this->ignorar = in_array("ignorar", $GLOBALS["argv"], true);
		$this->historico = in_array("historico", $GLOBALS["argv"], true);
		$this->desarrollo = in_array("desarrollo", $GLOBALS["argv"], true);

		if ($this->ignorar){
			$this->consulta("SET SESSION sql_mode=''");
		}

		//obtiene numero de procesamiento:
		$this->depurar("   Definiendo numero de procesamiento actual...");

		if ($this->client)
			$this->numeroProcesamiento();


		// Define si se está en modo de desarrollo en la migración.
		// Controla la ejecución de algunos métodos solo útiles durante el desarrollo.
		// Cuando se pase a producción, el valor debe cambiar a FALSE
		$this->desarrolloMig = true;

        // Este array contendrá las tablas reestructuradas, si ya se encuentra una tabla, entonces no la reestructurará
        $this->tablasReestructuradas = array();

        if ($this->desarrollo == true) {
        	$this->mensaje("Test -  Modo Desarrollo");

        	define("RAIZ_S", "D:");
			define("PROCESS_WAIT_NAME", "/respaldo/convertidos/deldia");
        }
	}

	protected function limpiaDirectorio($contador, $directorio, $recursivo, $nivel = 0, $terminando) {
		$this->depurar(__METHOD__."($contador, $directorio, $recursivo, $nivel)");

		if (@count($archivos = $this->listaArchivos(null, $directorio))) {
			$this->mensaje("      ".str_repeat(" ", $nivel * 3)."Limpiando archivos temporales...");

			foreach ($archivos as $archivo) {
				if ($this->client && $contador == 2) {
					if ($terminando) {
						$destino = SW_TODAY;
					} else {
						$destino = SW_WAIT;
					}

					$this
						->mensaje("        ".str_repeat(" ", $nivel * 3)."Moviendo el archivo '$archivo' a '$destino'...")
						->mueveArchivo($directorio."/".$archivo, $destino."/".$archivo);
				} else {
					$this
						->mensaje("         ".str_repeat(" ", $nivel * 3)."Eliminando el archivo '$archivo'...")
						->eliminaArchivo($archivo, $directorio."/");
				}

			}
		}

		if ($recursivo)
			if (!is_null($subdirectorios = $this->listaDirectorios($directorio)))
				foreach ($subdirectorios as $subdirectorio) {
					$this->mensaje("      ".str_repeat(" ", $nivel * 3)."Eliminando el directorio '$subdirectorio'...");

					$this
						->limpiaDirectorio($contador, $directorio."/".$subdirectorio, $recursivo, $nivel + 1, $terminando)
						->eliminaDirectorio($directorio."/".$subdirectorio);
				}

		return $this;
	}

	protected function identificaCliente($error = true) {
		$this->depurar(__METHOD__."($error)");

		if (!$this->client || !$this->identificado)
			$this->mensaje("   Identificando al cliente...");

		if (!$this->paquete)
			if (@count($paquetes = $this->listaArchivos(".zip", SW_WORKING)))
				$this->paquete = $paquetes[0];

		if (!$this->client)
			$this->client = (int)substr($this->paquete, 0, 4);

		$this->branch = (int)substr($this->paquete, 4, 2);
		if ($this->dia = (int)substr($this->paquete, 6, 2))
			$this->fecha_paquete = strtotime(date("Y-m-".$this->dia, $this->hoy));

		if ($this->client) {
			if (!$this->identificado) {
				$this->identificado = true;

				$this
					->mensaje("      Cliente '{$this->client}'\n")
					->usarBaseDatos(PREFIJO_SIM.$this->client, true)
					->usarBaseDatos(PREFIJO_SND.$this->client, true);

				$consulta = "
					SELECT g.paquetes AS total_paquetes,
						if(ifnull(g.Meses14, 0) = 0, 4, 14) AS meses_proceso,
						g.TotalMeses AS meses_detalles,
						c.Industry AS industria, c.Pais AS pais
					FROM crm_simetrical.groups AS g
					INNER JOIN crm_simetrical.clients AS c
						USING (Client)
					WHERE g.Client = ".$this->client;
				if ($resultado = $this->consulta($consulta)) {
					$campos = $resultado->fetch_assoc();
					$resultado->close();

					foreach ($campos as $campo => $valor)
						$this->$campo = $valor;

					if (!$this->meses_detalles)
						$this->meses_detalles = 3;

					if (!$this->industria)
						$this->industria = 501;
				}

				$divisiones = array();

				$consultaCrm = "
					SELECT *
					FROM crm_simetrical.clients AS c
					LEFT OUTER JOIN indice.marcas AS m
						ON c.Made = m.Marca
					WHERE c.Client = {$this->client}
						AND ifnull(c.Active, '') = ''";

				$consulta = $consultaCrm . " ORDER BY c.Branch";
				if ($resultado = $this->consulta($consulta)) {
					while ($fila = $resultado->fetch_assoc()) {
						$this->sucursales[$fila["Branch"]] = $fila;

						if ($fila["Division"])
							$divisiones[$fila["Division"]]["Sucursales"][$fila["Branch"]] = $fila;
					}
					$resultado->close();

					if (count($divisiones)) {
						ksort($divisiones);

						$identificador = 7000;
						foreach ($divisiones as $division => $parametros) {
							$this->divisiones[$identificador]["Nombre"] = $division;
							$this->divisiones[$identificador]["Sucursales"] = $parametros["Sucursales"];

							$identificador += 1;
						}
					}
					$this->total_paquetes = count($this->sucursales);				

					$this
						->depurar(print_r($this->sucursales, true))
						->depurar(print_r($this->divisiones, true))
						->depurar($this->total_paquetes);
				}

				$consulta = $consultaCrm . " ORDER BY FIELD(c.Made, 'FCO', 'RCO', 'JAC', 'Isuzu', 'Suzuki', 'Renault', 'Volvo', 'Peugeot', 'Chrysler', 'Infiniti', 'Nissan') DESC, c.Branch";
				if ($resultado = $this->consulta($consulta)) {
					while ($fila = $resultado->fetch_assoc()) {

						if (str_replace(" ", "_", strtolower($fila["Made"])) == 'mercedes_benz') {
							$marca = strtoupper(str_replace(" ", "", substr($fila["Made"], 0, 8)));
						} else {
							$marca = strtoupper(str_replace(" ", "_", $fila['Made']));
						}

						$this->marcas[(int)$fila["MadeId"]]["Nombre"] = trim($fila["Made"]);
						$this->marcas[(int)$fila["MadeId"]]["NombreCorto"] = $marca;
						$this->marcas[(int)$fila["MadeId"]]["Sucursales"][$fila["Branch"]] = $fila;
					}
					$resultado->close();				

					$this->depurar(print_r($this->marcas, true));
				}
			}
		} else {
			if ($error)
				$this->error("No se logro identificar al cliente");
		}

        if (empty($this->sucursales)) {
            // El cliente no tiene sucursales activas, no debe continuar
            // Se procede a eliminar el archivo version para que el PrdServer se apague
            // Se descargan los paquetes para que no queden en 'convertidos' y no se intente procesar el cliente de nuevo
            $this->guardaLog(['01x0101SINC', 'El cliente no cuenta con sucursales activas']);
            $this->descargaPaquetes();
            $this->eliminaArchivo('/simetrical/version.txt', null);
            $this->error('El cliente no cuenta con sucursales activas');
        }

		return $this;
	}

	protected function limpiaEspacio($mantener = false, $terminando = false) {
		$this->depurar(__METHOD__."($mantener, $terminando)");

		$this->mensaje("Preparando el espacio de trabajo...");

		if ($mantener)
			$this->identificaCliente(false);

		$directorios = array(
			SW_WAIT,
			SW_WORKING,
			SW_SANDBOX,
			SW_TRANSFER,
			SW_PASO,
			SW_UPLOAD,
			SW_TODAY,
			SW_HOLD,
			SW_MESSAGES,
			SW_SPECS,
			);

		if (!$this->desarrollo) {
			array_push($directorios, RAIZ_SMIG);
			array_push($directorios, RAIZ_SW);
		}

		$libres = array(1, 7, 8);

		$recursivos = array(3, 11);

		if ($this->client) {
			$libres[] = 0;
			$libres[] = 10;
			$libres[] = 11;
		} else {
			$this->eliminaArchivo(TEMP."/sincronizado.txt", null);
		}

		if ($mantener)
			$libres[] = 2;

		$contador = 0;
		foreach ($directorios as $directorio) {
			$this
				->mensaje("   Creando el directorio '$directorio'...")
				->creaDirectorio($directorio);

			if (!in_array($contador, $libres))
				$this->limpiaDirectorio($contador, $directorio, in_array($contador, $recursivos), 0, $terminando);

			$contador++;
		}

		$this->mensaje();

		if ($this->client) {
			$this->mensaje("   Limpiando las bases de datos...");

			foreach (array(PREFIJO_SND.$this->client, PREFIJO_SC.$this->client) as $basedatos) {
				$this->mensaje("      '$basedatos'...");

				$consulta = "DROP DATABASE IF EXISTS $basedatos";
				$this->consulta($consulta);
			}

			$this->mensaje();
		}

		$this->mensaje("   Limpiando las variables...");

		$this->branch = null;
		$this->paquete = null;
		$this->dia = null;
		$this->fecha_paquete = $this->hoy;

		$this->mensaje();

		return $this;
	}

	protected function sincronizaCliente2() {
		$this->depurar(__METHOD__);

		if (!$this->existeArchivo(TEMP."/sincronizado.txt", null)) {
			$this
				->mensaje("Sincronizando Client...")
				->identificaCliente();
			$this->guardaLog(['01x0102SINC', 'Sincronizacion del Cliente']);

			if ($this->client) {
				$directorio =RAIZ_E."/".$this->client;
				if ($this->sincronizar) {
					$mysqldump = "mysqldump";
					$mysql = "mysql";
					if (SO == "WINDOWS") {
						$mysqldump = "%mysqldump%";
						$mysql = "%mysql%";
					}

					$tablas = $this->listaArchivos(".dump",$directorio);

					$this->decodificaDSN($this->dsn,
						$servidor, $usuario, $contrasena, $basedatos, $puerto);

					$this->mensaje("   Sincronizando la base de datos historica...");
					$this->mensaje();

					if ($this->dump) {
						if ($this->existeArchivo($this->client.".sql.dump", RAIZ_E."/")) {
							$comando = "$mysql -h $servidor -P $puerto -u $usuario -p$contrasena -e \"DROP DATABASE IF EXISTS ".PREFIJO_SIM.$this->client."; CREATE DATABASE ".PREFIJO_SIM.$this->client."\"";
							$this->ejecutaComando($comando);

							$comando = "$mysql ".PREFIJO_SIM.$this->client." -h $servidor -P $puerto -u $usuario -p$contrasena --force < ".RAIZ_E."/".$this->client.".sql.dump";

							$this->ejecutaComando($comando);

							sleep(1);
							if (!$this->existeTabla(PREFIJO_SIM.$this->client. '.tablas')){
								$this->mensaje("\n   El cliente no tiene configurado 'tablas'...");
								$this->clonaTabla("indice.auto-tbl", PREFIJO_SIM.$this->client.".tablas");
								$this->mensaje("      Obteniendo 'tablas' de 'auto_tbl'...\n");
								$this->auto_tbl = 1;
							}

							$this->sincronizaTablasConfiguracion("*");

						} else {
							$consulta = "DROP DATABASE IF EXISTS ".PREFIJO_SIM.$this->client."; CREATE DATABASE ".PREFIJO_SIM.$this->client;
							$comando = "$mysql -h $servidor -P $puerto -u $usuario -p$contrasena -e\"$consulta\"";

							if($this->ejecutaComando($comando)){
								$this->mensaje("      Eliminando Base de Datos anterior...");
							}

							$this->mensaje();

							$this->mensaje("      Importando...");
							foreach ($tablas as $value => $tabla) {
								if ($this->existeArchivo($tabla,$directorio."/")) {
									$this->mensaje("         '".substr($tabla,0,-9)."'");
									$comando = "$mysql ".PREFIJO_SIM.$this->client." -h $servidor -P $puerto -u $usuario -p$contrasena --force < $directorio/$tabla";
									$this->ejecutaComando($comando);
								}
							}
							sleep(1);
							if (!$this->existeTabla(PREFIJO_SIM.$this->client. '.tablas')){
								$this->mensaje("\n   El cliente no tiene configurado 'tablas'...");
								$this->clonaTabla("indice.auto-tbl", PREFIJO_SIM.$this->client.".tablas");
								$this->mensaje("      Obteniendo 'tablas' de 'auto_tbl'...\n");
								$this->auto_tbl = 1;
							}
						}
					} else {
						$this->decodificaDSN($this->dsn_remoto,
							$servidor, $usuario, $contrasena, $basedatos, $puerto);
						$comando = "$mysqldump -h $servidor -P $puerto -u $usuario -p$contrasena --compress --compatible=ansi --skip-triggers --databases ".PREFIJO_SIM.$this->client;

						$this->decodificaDSN($this->dsn,
							$servidor, $usuario, $contrasena, $basedatos, $puerto);
						$comando = "$comando | $mysql -h $servidor -P $puerto -u $usuario -p$contrasena --force";

						$this->ejecutaComando($comando);
					}

					$this->mensaje();

					$this->mensaje("   Sincronizando las bases de datos de plantas...");

					foreach ($this->marcas as $marca => $parametros) {
						$this->mensaje("      Sincronizando '{$parametros["Nombre"]}'...");

						$fabricante = str_replace(" ", "_", strtolower($parametros["Nombre"]));

						if ($this->dump) {
							if (count($dumps = $this->listaArchivos(".dump", RAIZ_E."/mfr_".$fabricante))) {
								$comando = "$mysql -h $servidor -P $puerto -u $usuario -p$contrasena -e \"DROP DATABASE IF EXISTS mfr_$fabricante; CREATE DATABASE mfr_$fabricante\"";

								$this->ejecutaComando($comando);

								foreach ($dumps as $dump) {
									$comando = "$mysql mfr_$fabricante -h $servidor -P $puerto -u $usuario -p$contrasena --force < ".RAIZ_E."/mfr_$fabricante/".$dump;

									$this->ejecutaComando($comando);
								}
							}

							if ($fabricante == "ncl" || $fabricante == "nar" || $fabricante == "npe" || $fabricante == "npa") {
								$this->mensaje("       Sincronizando base de datos de NLAC");

								if (count($dumps = $this->listaArchivos(".dump", RAIZ_E."/mfr_nlac"))) {
									$comando = "$mysql -h $servidor -P $puerto -u $usuario -p$contrasena -e \"DROP DATABASE IF EXISTS mfr_nlac; CREATE DATABASE mfr_nlac\"";

									$this->ejecutaComando($comando);

									foreach ($dumps as $dump) {
										$comando = "$mysql mfr_nlac -h $servidor -P $puerto -u $usuario -p$contrasena --force < ".RAIZ_E."/mfr_nlac/".$dump;

										$this->ejecutaComando($comando);
									}
								}
							}
						} else {
							$this->decodificaDSN($this->dsn_remoto,
								$servidor, $usuario, $contrasena, $basedatos, $puerto);
							$comando = "$mysqldump -h $servidor -P $puerto -u $usuario -p$contrasena --compress --compatible=ansi --skip-triggers --databases mfr_$fabricante";

							$this->decodificaDSN($this->dsn,
								$servidor, $usuario, $contrasena, $basedatos, $puerto);
							$comando = "$comando | $mysql -h $servidor -P $puerto -u $usuario -p$contrasena --force";

							$this->ejecutaComando($comando);
						}

						$this->mensaje();
					}

					$this->escribeArchivo(TEMP."/sincronizado.txt", null, date("Y-m-d H:i:s"));
				}
			} else {
				$this->mensaje("   No se ha definido un cliente");
			}

			$this
				->mensaje()
				->mensaje("Hora: ".date("H:i:s"))
				->mensaje();

			$this->guardaLog(['02x0102SINC', 'Sincronizacion del Cliente']);
		}

		return $this;
	}

	protected function ObtieneDumps($clientes = array(), $esPlanta = false)  {
		$dumps = array();

		if (count($clientes) > 0) {
			foreach($clientes as $cliente) {
				if (SO == 'WINDOWS') {
						$dir_cliente = $esPlanta ? SD_MFR . "/" . $cliente : RAIZ_S . DB ."/". $cliente;
					} else {
						$dir_cliente = $esPlanta ? RAIZ_E ."/mfr_" . $cliente : RAIZ_E ."/". $cliente;
					}

				if ($this->existeDirectorio($dir_cliente)) {
					if (count($archivos = $this->listaArchivos(".sql.dump", $dir_cliente))) {
						foreach($archivos as $archivo) {
							if ($this->existeArchivo($dir_cliente."/".$archivo, null)) {
								$this->depurar("existe $archivo");
								$dumps[$dir_cliente."/".$archivo] = filesize($dir_cliente."/".$archivo);
							}
						}
					}
					$this->continua = 1;
				} else {
					$this->mensaje("    Sin contrato...");
					$this->continua = 0;
				}
			}
		}

		$this->depurar(print_r($dumps,true));
		// Ordena los dumps por peso
		arsort($dumps);
		$this->depurar(print_r($dumps,true));
		return $dumps;
	}

	protected function sincronizaCliente() {
		$this->depurar(__METHOD__);

		$mysqldump 		= (SO == "WINDOWS") ? '%mysqldump%' : 'mysqldump';
		$mysql 			= (SO == "WINDOWS") ? '%mysql%' : 'mysql';

		$this
			->mensaje("Sincronizando Cliente...")
			->mensaje("Hora: ".date("H:i:s"))
			->mensaje();

		$this->guardaLog(['01x0102SINC', 'Sincronizacion del Cliente']);

		if (!$this->existeArchivo(TEMP."/sincronizado.txt", null)) {
			$this->decodificaDSN($this->dsn,$servidor, $usuario, $contrasena, $basedatos, $puerto);
			$this->identificaCliente();

			$this->mensaje("Cliente : ".$this->client);
			$comando = "$mysql -h $servidor -P $puerto -u $usuario -p$contrasena -e \"DROP DATABASE IF EXISTS ".PREFIJO_SIM.$this->client."; CREATE DATABASE ".PREFIJO_SIM.$this->client."\"";

			if($this->ejecutaComando($comando))
				$this->mensaje("Limpiando base de datos...");

			$clientes 	= array($this->client);
			$dumps 		= $this->ObtieneDumps($clientes, false);
			$dumps 		= array_keys($dumps);
			$directorioLog	= TEMP . "/SincronizaDump/{$this->client}";

			if (!$this->existeDirectorio($directorioLog)) {
				$this->creaDirectorio($directorioLog);
			}

			$this->mensaje('Sincronizando tablas a base de datos local '.PREFIJO_SIM.$this->client);
			if (!empty($dumps)) {
				$contador = 0;
				foreach ($dumps as $dump) {
					if (SO == "WINDOWS") {
						if ($contador == 75) {
							$this->mensaje("\nBalanceando importancion...");
							sleep(30);
						}
					} else {
						if ($contador == 90) {
							$this->mensaje("\nBalanceando importancion...");
							sleep(30);
						}
					}
	
					$cmd = "$mysql ".PREFIJO_SIM.$this->client." -h $servidor -P $puerto -u $usuario -p$contrasena --force < $dump";
					if (SO == 'LINUX') {
						$cmd .= " > $directorioLog/".substr(strrchr($dump, '/'), 1, -9).".log 2>&1 &";
						$this->ejecutaComando($cmd , false);
					} else {
						$cmd = 'start "" /B '. $cmd;
						pclose(popen($cmd, "r"));
					}
	
					echo ".";
					$contador++;
				}
			} else {
				$this->error('	No existe ningun dump para importar');
				$this->guardaLog(['01x0101IMDP', 'No existe ningun dump para importar']);
			}

			$procesos_corriendo = 0;
			$cont_procesos_cero = 0;

			$this->mensaje();
			$this->mensaje('Esperando que finalice la sincronización...');
			do {
				$consulta = " SELECT * FROM INFORMATION_SCHEMA.PROCESSLIST WHERE DB = '".PREFIJO_SIM.$this->client."' AND INFO LIKE 'INSERT INTO%'";
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

			sleep(1);
			if (!$this->existeTabla(PREFIJO_SIM.$this->client. '.tablas')){
				$this->mensaje("\n El cliente no tiene configurado 'tablas'...");
				$this->clonaTabla("indice.auto-tbl", PREFIJO_SIM.$this->client.".tablas");
				$this->mensaje("   Obteniendo 'tablas' de 'auto_tbl'...\n");

				if (SO == 'LINUX') {
					$this->auto_tbl = 1;
				} else {
					if ($this->existeArchivo($this->client."_auto-table.txt",RAIZ_S.DB."/"))
						$this->eliminaArchivo($this->client."_auto-table.txt",RAIZ_S.DB."/");
						$this->escribeArchivo(RAIZ_S.DB."/".$this->client."_auto-table.txt", null, date("Y-m-d"));
				}
			}

			$this->mensaje();

			$dir = (SO == 'WINDOWS') ? RAIZ_S . DB ."/" : RAIZ_E . "/";
			$this->ejecutaValidacionSinc($dir.$this->client, PREFIJO_SIM.$this->client, $directorioLog);
			$this->mensaje('Sincronizacion del cliente concluida');

			$this->mensaje();
			if (!$this->existeArchivo("no_plantas.txt" , RAIZ_S.'/')) {  // si colocamos el archivo  D:/no_plantas.txt este no sicronizara las plantas del cliente
				foreach ($this->marcas as $marca => $parametros) {
					$fabricante 	= str_replace(" ", "_", strtolower($parametros["Nombre"]));
					$fabricante 	= str_replace("-", "_", strtolower($fabricante));
					if ($fabricante == "ncl" || $fabricante == "nar" || $fabricante == "npe" || $fabricante == "npa")
						$this->marcas['8260']['Nombre'] = 'nlac';
						$this->esNlac = 1;
				}

				foreach ($this->marcas as $marca => $parametros) {
					$this->mensaje();
					$this->mensaje("Sincronizando '{$parametros["Nombre"]}'...");

					$fabricante 	= str_replace(" ", "_", strtolower($parametros["Nombre"]));
					$fabricante 	= str_replace("-", "_", strtolower($fabricante));
					$plantas		= array($fabricante);
					$dumps 			= $this->ObtieneDumps($plantas, true);
					$dumps 			= array_keys($dumps);
					$directorioLog	= TEMP . "/SincronizaDump/mfr_$fabricante";

					if (!$this->existeDirectorio($directorioLog)) {
						$this->creaDirectorio($directorioLog);
					}

					if ($this->continua == 1 ) {
						$this->mensaje("Limpiando base de datos...");
						$comando = "$mysql -h $servidor -P $puerto -u $usuario -p$contrasena -e \"DROP DATABASE IF EXISTS mfr_$fabricante; CREATE DATABASE mfr_$fabricante\"";
						$this->ejecutaComando($comando);

						$this->mensaje("Sincronizando tablas a base de datos local mfr_$fabricante...");
						$contador = 0;
						foreach ($dumps as $dump) {
							if ( ($fabricante == 'infiniti') or ($fabricante == 'fco') or ($fabricante == 'nissan') or ($fabricante == 'chrysler') or ($fabricante == 'renault') ) {
								if ($contador == 30) {
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
								$cmd .= " > $directorioLog/".substr(strrchr($dump, '/'), 1, -9).".log 2>&1 &";
								$this->ejecutaComando($cmd , false);
							}

							echo ".";
							$contador++;
						}

						$procesos_corriendo = 0;
						$cont_procesos_cero = 0;

						$this->mensaje("\nEsperando que finalice la sincronización...");
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
					$this->ejecutaValidacionSinc($dir.$fabricante, "mfr_$fabricante", $directorioLog);

					$this->mensaje();
				}
			}

			$this->mensaje('Sincronizacion concluida');

			$this->identificaCliente();
			$this->escribeArchivo(TEMP."/sincronizado.txt", null, date("Y-m-d H:i:s"));

			$this->kpibulk();
		}

		$this
			->mensaje()
			->mensaje("Hora: ".date("H:i:s"))
			->mensaje();

		if ($this->esNlac == 1)
			unset($this->marcas['8260']);

		$this->guardaLog(['02x0102SINC', 'Sincronizacion del Cliente']);

		$this->mensaje();
		return $this;
	}

	protected function validaSincronizacion(){
		$this->depurar(__METHOD__);
		$this->mensaje("Validando Sincronizacion de tablas");

		$this->mensaje("   Validando base de datos del cliente...");
		$directorio = RAIZ_E . "/" . $this->client;
		$this->ejecutaValidacionSinc($directorio, PREFIJO_SIM.$this->client);

		$this->mensaje();
		return $this;
	}

	protected function ejecutaValidacionSinc($directorioDump, $basedatos, $directorioLog = null) {
		$this->depurar(__METHOD__."($directorioDump, $basedatos, $directorioLog)");

		$this->mensaje("\nValidando Sincronizacion de tablas...\n");
		$tablas = $this->listaArchivos('.dump', $directorioDump);

		foreach ($tablas as $id => $tabla) {
			$nombreTabla = substr($tabla, 0, -9);
			$existeError = false;

			$intentos = 3;
			$intentoActual = 0;
			$procesoExitoso = false;

			while ($intentoActual < $intentos && !$procesoExitoso) {
				$intentoActual++;
				
				$existeTabla = $this->existeTabla("$basedatos.$nombreTabla");

				if ($directorioLog) {
					$contenidoLog = substr($this->leeArchivo("$nombreTabla.log", $directorioLog), 0, 5);
					$existeError = $contenidoLog == 'ERROR';
				}

				if (SO == 'LINUX' && $existeError || SO == 'WINDOWS' && !$existeTabla) {
					$this->mensaje("	Intento #$intentoActual: $nombreTabla");
					sleep(60);
					$this->sincronizaTabla($basedatos, $tabla, $directorioDump, $directorioLog ? $directorioLog : null);
				} else {
					$procesoExitoso = true;
				}
			}

			if (!$procesoExitoso) {
				$this->mensaje("	No se pudo importar la tabla $basedatos.$nombreTabla");
				$this->mensaje();
				$this->guardaLog(['01x0101IMDP', "No se pudo importar la tabla $basedatos.$nombreTabla"]);
			}

		}

		$this->mensaje();
		return $this;
	}

	protected function sincronizaTabla($basedatos, $tabla, $directorio, $directorioLog = null) {
		$this->depurar(__METHOD__."($basedatos, $tabla, $directorio)");

		$mysqldump 		= (SO == "WINDOWS") ? '%mysqldump%' : 'mysqldump';
		$mysql 			= (SO == "WINDOWS") ? '%mysql%' : 'mysql';

		$this->decodificaDSN($this->dsn,$servidor, $usuario, $contrasena, $basedatos, $puerto);

		if ($this->existeArchivo($tabla ,$directorio."/")) {
			$comando = "$mysql $basedatos -h $servidor -P $puerto -u $usuario -p$contrasena --force < $directorio/$tabla";

			if (SO == 'LINUX' && $directorioLog) {
				$nombreTabla = substr($tabla, 0, -9);
				$comando .= " > $directorioLog/$nombreTabla.log 2>&1";
			}

			$this->ejecutaComando($comando);
		}

		return $this;
	}

	protected function sincronizaTablasConfiguracion($tablas) {
		$this->depurar(__METHOD__);
		$this->depurar($tablas);

		$listaTablas = ($tablas == '*') ? "galib,tablas,types,chart,lista,mac" : $tablas;

		$listaTablas = explode(",", $listaTablas);

		if ($this->client) {
			$directorio =RAIZ_E."/".$this->client;
			$mysqldump = "mysqldump";
			$mysql = "mysql";
			if (SO == "WINDOWS") {
				$mysqldump = "%mysqldump%";
				$mysql = "%mysql%";
			}

			$this->decodificaDSN($this->dsn,$servidor, $usuario, $contrasena, $basedatos, $puerto);

			$this->mensaje("   Sincronizando tablas de configuracion...");

			$this->mensaje("      Importando...");

			foreach ($listaTablas as $value => $tabla) {
				if ($this->existeArchivo($tabla.".sql.dump",$directorio."/")) {
					$this->mensaje("         '$tabla'");
					$comando = "$mysql ".PREFIJO_SIM.$this->client." -h $servidor -P $puerto -u $usuario -p$contrasena --force < $directorio/$tabla.sql.dump";
					$this->ejecutaComando($comando);
				}
			}
		}

		$tablas = null;
		return $this;
	}

	protected function kpibulk(){
		$this->depurar(__METHOD__);
		$this->mensaje("Revisando si existen KPI nuevos bajo demanda...");

		$consulta = "
			SELECT DISTINCT Made
			FROM crm_simetrical.clients AS c
			LEFT OUTER JOIN indice.marcas AS m
				ON c.Made = m.Marca
			WHERE c.Client = {$this->client}
				AND ifnull(c.Active, '') = ''
			ORDER BY c.Branch";
		$resultado = $this->consulta($consulta);
		$marcas = "AND client = 0 AND Fabricante IN (";
		while ($fila = $resultado->fetch_assoc()) {
			$fabricante = $fila["Made"];
			$fabricante = str_replace(" ", "_", strtolower($fabricante));
			$marcas .= "'" . $fabricante . "'," ;
			$marcas .= "'" . strtoupper($fabricante) . "',";
		}

		$marcas = substr($marcas, 0, -1 ) . ")";

		$this->procesoExterno('despues', 'sincronizaCliente', 1 , $marcas);
		$this->mensaje();
		return $this;
	}


	protected function copiaEspecificaciones() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102CPES', 'Copia especificaciones']);

		$this->identificaCliente();

		if ($this->client) {
			$this->mensaje("      Copiando las especificaciones...");

			foreach (array("", "Batch", "Models", "Projects", "Scripts") as $directorio)
				$this->copiaArchivos(CLIENTS."/".$this->client."/$directorio".($directorio ? "/" : ""), SW_SPECS."/", false);
		}

		$this->guardaLog(['02x0102CPES', 'Copia especificaciones']);
		return $this;
	}

	protected function descargaPaquetes() {
		$this->depurar(__METHOD__);

		if ($this->descargar) {
			if ($this->client) {
				$this->mensaje("Descargando paquetes remotos para el cliente '{$this->client}'...");

				if (count($remotos = $this->listaArchivosRemotos(URI_FTP_PROCESS.PROCESS_WAIT_NAME))) {
					$paquetes = array();

					foreach ($remotos as $paquete) {
						if ((int)substr($paquete, 0, 4) == $this->client) {
							$this->mensaje("   Descargando '$paquete'...");

							$paquetes[] = $paquete;

							$this->descargaArchivoFTP(PROCESS_WAIT_NAME."/".$paquete,
								URI_FTP_PROCESS, SW_WAIT."/".$paquete, true);
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

	protected function descomprimePaquete() {
		$this->depurar(__METHOD__);
		$descomprimir = false;

		if (!$this->client || !$this->paquete)
			$this->identificaCliente();

		if ($this->client && $this->paquete) {
			$this->mensaje('       Descomprimiendo paquete ' . $this->paquete . '...');
			$descomprimir = $this->descomprimeArchivoSC(SW_WORKING . '/' . $this->paquete, SW_SANDBOX);
		}

		return $descomprimir;
	}

	protected function transformaTablas() {
		$this->depurar(__METHOD__);

		if ($this->transformar) {
			$this->identificaCliente();

			if ($this->client && $this->paquete)
				if (SO == "WINDOWS")
					if (!$this->existeArchivo("convertido.txt", SW_SANDBOX."/"))
						if ($this->existeArchivo("20-MONAR.bat", RAIZ_SW."/")) {
							$this->mensaje("      Transformando las tablas...");

							$this->ejecutaComando(RAIZ_SW."/20-MONAR.bat", true, true);

							$this->mensaje();
						}
		}

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

			$consulta = "DROP DATABASE IF EXISTS ".PREFIJO_SND.$this->client;
			$this->consulta($consulta);

			$archivos_dbs = $this->listaArchivos('.db', SW_SANDBOX.'/');
			$archivosConfig = array('mac', 'tablas', 'chart', 'galib', 'types');

			foreach ($archivosConfig as $archivo) {
				$pattern = "/^".strtoupper($archivo).".DB(\d*).db/";
				if ($db = preg_grep($pattern, $archivos_dbs)) {
					$this->eliminaArchivo(implode(',', $db), SW_SANDBOX . '/');
				}
				$this->eliminaArchivo(strtoupper($archivo).".DB", SW_SANDBOX . '/');
			}

			$comando = "$php -f ".dirname(__FILE__)."/pdx2mysql.php -- $dsn directorio=".SW_SANDBOX." basedatos=".PREFIJO_SND.$this->client." limpiar $depurar";
			$this->ejecutaComando($comando, true, true);
		}

		return $this;
	}

    /**
	 * Parte de 3-CONSOL.SC
	 *
	 * Método que reestructura las tabla indicada de PREFIJO_SIM_<cliente> con respecto
	 * a como queda en snd_<cliente>.
     *
     * @param str $tablaOrigen
     * @param str $tablaDestino
     * @return void
	 */
	protected function reestructuraTabla($tablaOrigen = null, $tablaDestino = null, $tipo = null) {
        $this->depurar(__METHOD__);
        $tablaOrigen = !empty($tablaOrigen) ? $tablaOrigen : $this->tablaO;
        $tablaDestino = !empty($tablaDestino) ? $tablaDestino : $this->tablaD;

        // Validar si la tabla ya fue reestructurada durante el procesamiento
        if (in_array($tablaDestino, $this->tablasReestructuradas)) {
            return $this;
        }

        if ($this->client && !empty($tablaOrigen) && !empty($tablaDestino)) {
            $this->mensaje('      Reestructurando tabla ' . $tablaDestino . '...');
            $baseDatos =  $this->baseDatosActual();
            $baseDatosPrincipal = PREFIJO_SIM . $this->client;
            $baseDatosSecundaria = PREFIJO_SND . $this->client;

            if ($this->existeTabla($baseDatosPrincipal . '.' . $tablaDestino) && $this->existeTabla($baseDatosSecundaria . '.' . $tablaOrigen)) {
                // Identificar campos a eliminar de PREFIJO_SIM.<tabla>
                $consultaCamposDrop = '
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = "' . $tablaDestino . '"
                    AND TABLE_SCHEMA = "' . $baseDatosPrincipal . '"
                    AND COLUMN_NAME NOT IN (
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = "' . $tablaOrigen . '"
                        AND TABLE_SCHEMA = "' . $baseDatosSecundaria . '"
                    )
                ';
                $resultadoCamposDrop = $this->consulta($consultaCamposDrop);

                $controlDrop = false;

                if ($resultadoCamposDrop && $controlDrop) {
                    $campos = $resultadoCamposDrop->fetch_all(MYSQLI_ASSOC);
                    $resultadoCamposDrop->close();
                    $excluirCamposDrop = array('IdRegistro');

                    if (!empty($campos)) {
                        $campos = array_map(
                            function ($campo) use($excluirCamposDrop) {
                                return in_array($campo, $excluirCamposDrop) ? '' : ' DROP `' . $campo . '`';
                            },
                            array_column($campos, 'COLUMN_NAME')
                        );
                        $campos = array_filter($campos);
                        $this->mensaje('        Eliminando campos de ' . $baseDatosPrincipal . '.' . $tablaDestino . ':' . implode(',', $campos));
                        $consultaDrop = 'ALTER TABLE ' . $tablaDestino . implode(',', $campos);
                        $this->usarBaseDatos($baseDatosPrincipal);
                        $this->consulta($consultaDrop);
                    }
                }

                $consultaCamposAdd = '
                    SELECT COLUMN_NAME, COLUMN_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = "' . $tablaOrigen . '"
                    AND TABLE_SCHEMA = "' . $baseDatosSecundaria . '"
                    AND COLUMN_NAME NOT IN (
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = "' . $tablaDestino . '"
                        AND TABLE_SCHEMA = "' . $baseDatosPrincipal . '"
                    )
                ';
                $resultadoCamposAdd = $this->consulta($consultaCamposAdd);

                if ($resultadoCamposAdd) {
                    $campos = $resultadoCamposAdd->fetch_all(MYSQLI_ASSOC);
                    $resultadoCamposAdd->close();

                    if (!empty($campos)) {
                        $campos = array_column($campos, 'COLUMN_TYPE', 'COLUMN_NAME');

                        // Si el reporte es de tipo R o S, no se le agrega el campo Date
                        if ($tipo == 'R' || $tipo == 'S') {
                            if (array_key_exists('Date', $campos)) {
                                unset($campos['Date']);
                            }
                        }

                        array_walk(
                            $campos,
                            function (&$campoValor, $campoKey) {
                                $campoValor = ' ADD `' . $campoKey . '` ' . $campoValor;
                            }
                        );
                        $this->mensaje('        Agregando campos de ' . $baseDatosPrincipal . '.' . $tablaDestino . ':' . implode(',', $campos));
                        $consultaAdd = 'ALTER TABLE ' . $tablaDestino . implode(',', $campos);
                        $this->usarBaseDatos($baseDatosPrincipal);
                        $resultadoAdd = $this->consulta($consultaAdd);
                    }
                }

                // Se reordenan los campos y se modifica el tipo de dato
                $consultaCamposModificar = '
                    SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, EXTRA
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = "' . $tablaOrigen . '"
                    AND TABLE_SCHEMA = "' . $baseDatosSecundaria . '"
                ';
                $resultadoCamposModify = $this->consulta($consultaCamposModificar);

                if ($resultadoCamposModify) {
                    $campos = $resultadoCamposModify->fetch_all(MYSQLI_ASSOC);
                    $resultadoCamposModify->close();

                    if (!empty($campos)) {
                        $first = true;
                        $campoPrevio = null;
                        $arregloModify = array();

                        // Validar la existencia del campo IdRegistro y dejarlo al inicio
                        if ($this->existeCampo('IdRegistro', $baseDatosPrincipal . '.' . $tablaDestino)) {
                            $first = false;
                            $campoPrevio = 'IdRegistro';
                        }

                        foreach ($campos as $campo) {
                            // El campo IdRegistro no se modifica, se salta
                            if ($campo['COLUMN_NAME'] == 'IdRegistro') {
                                continue;
                            }

                            // Si el reporte es de tipo R o S, no se agrega el campo Date a la modificación
                            if ($tipo == 'R' || $tipo == 'S') {
                                if ($campo['COLUMN_NAME'] == 'Date') {
                                    continue;
                                }
                            }

							// Si la cadena del origen es más pequeña que la del destino, no se modifica el tipo, para evitar truncar datos
							if (!stristr($campo['COLUMN_TYPE'], 'varchar') === FALSE) {
								$consulta = "
									SELECT COLUMN_TYPE
									FROM INFORMATION_SCHEMA.COLUMNS
									WHERE TABLE_SCHEMA = '$baseDatosPrincipal'
									AND TABLE_NAME = '$tablaDestino'
									AND COLUMN_NAME = '{$campo['COLUMN_NAME']}'";

								$resultTipoDestino = $this->consulta($consulta);
								$tipoDestino = $resultTipoDestino->fetch_all(MYSQLI_ASSOC);
								$resultTipoDestino->close();
								$tipoDestino = $tipoDestino[0]['COLUMN_TYPE'];

								$longitudTipoDestino = (int) filter_var($tipoDestino, FILTER_SANITIZE_NUMBER_INT);
								$longitudTipoOrigen  = (int) filter_var($campo['COLUMN_TYPE'], FILTER_SANITIZE_NUMBER_INT);

								$campo['COLUMN_TYPE'] = $longitudTipoOrigen < $longitudTipoDestino ? $tipoDestino : $campo['COLUMN_TYPE'];
							}

                            $posicion = $first ? 'FIRST ' : 'AFTER ';
                            $nulo = $campo['IS_NULLABLE'] == 'NO' ? 'NOT NULL ' : '';
                            $extra = !empty($campo['EXTRA']) ? $campo['EXTRA'] . ' ' : '';
                            $campoPrevio = !empty($campoPrevio) ? '`' . $campoPrevio . '`' : null;
                            $arregloModify[] = ' MODIFY `' . $campo['COLUMN_NAME'] . '` ' . $campo['COLUMN_TYPE'] . ' ' . $nulo . $extra . $posicion . $campoPrevio;
                            $campoPrevio = $campo['COLUMN_NAME'];
                            $first = false;
                        }

                        $this->mensaje('        Modificando campos de ' . $baseDatosPrincipal . '.' . $tablaDestino);
                        $consultaModify = 'ALTER TABLE ' . $tablaDestino . implode(',', $arregloModify);
                        $this->usarBaseDatos($baseDatosPrincipal);
                        $resultadoModify = $this->consulta($consultaModify);
                    }
                }

                $this->tablasReestructuradas[] = $tablaDestino;
            } else {
                $this->mensaje('            No hay tabla para reestructurar.');
            }

            $this->usarBaseDatos($baseDatos);
        }

        return $this;
    }

    protected function eliminaRegistrosDuplicados($tabla = null) {
        $this->depurar(__METHOD__);

        if (empty($tabla)) {
            return $this;
        }

        $baseDatos = $this->baseDatosActual();
        $this->usarBaseDatos(PREFIJO_SND . $this->client);

        if ($this->existeTabla(PREFIJO_SND . '.' . $tabla) && $this->client) {
            // Se obtienen las columnas de la tabla
            $consultaCampos = '
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = "' . $tabla . '"
                AND TABLE_SCHEMA = "' . PREFIJO_SND . $this->client . '"
            ';
            $resultadoCampos = $this->consulta($consultaCampos);

            if ($resultadoCampos) {
                $campos = $resultadoCampos->fetch_all(MYSQLI_ASSOC);
                $resultadoCampos->close();
                $campos = array_map(
                    function ($campo) {
                        return '`' . $campo . '`';
                    },
                    array_column($campos, 'COLUMN_NAME')
                );

                // Renombrar la tabla, insertar los datos nuevamente en $tabla
                $tablaPaso = 'tmp_' . $tabla . '_completa';
                $this->eliminaTabla($tablaPaso);
                $this->renombraTabla($tabla, $tablaPaso);
                $consultaCreaTabla = 'CREATE TABLE ' . $tabla . ' LIKE ' . $tablaPaso;
                $resultadoCreaTabla = $this->consulta($consultaCreaTabla);

                if ($resultadoCreaTabla) {
                    // Insertar los datos únicos
                    $consultaInsert = '
                        INSERT INTO ' . $tabla . '
                        SELECT *
                        FROM ' . $tablaPaso . '
                        GROUP BY ' . implode(', ', $campos) . '
                    ';
                    $resultadoInsert = $this->consulta($consultaInsert);

                    if ($resultadoInsert) {
                        $this->mensaje('      ' . $tabla . ' actualizada con GROUP BY');
                        $this->eliminaTabla($tablaPaso);
                    }
                }
            }
        }

        $this->usarBaseDatos($baseDatos);
        return $this;
    }

	protected function consolidaTablas() {
		$this->depurar(__METHOD__);

		$this->identificaCliente();
		$this->guardaLog(['01x010330CO', 'Batch 30-CONSOL', null, $this->paquete]);

		if ($this->client) {
			if ($this->existeArchivo("consolida.txt", SW_SPECS."/")) {

			} else {
				if ($this->existeArchivo("consolida_antes.txt", SW_SPECS."/")) {
				}

				$this->usarBaseDatos(PREFIJO_SND.$this->client);

				if (count($tablas = $this->listaTablas(array('excluir' => 'tablas,lista,galib,types,mac,chart')))) {
					$this->depurar(print_r($tablas, true));

					if (!$this->existeTabla("tablas"))
						if ($this->existeTabla(PREFIJO_SIM.$this->client.".tablas")) {
							$this->clonaTabla(PREFIJO_SIM.$this->client.".tablas", "tablas");
						} else {
							$this->clonaTabla("indice.auto-tbl", "tablas");
						}

					$consulta = "
						CREATE TEMPORARY TABLE IF NOT EXISTS lista (
						Client SMALLINT UNSIGNED,
						Branch SMALLINT UNSIGNED,
						PRIMARY KEY (Client, Branch))";
					$this->consulta($consulta);

					foreach ($tablas as $tabla => $propiedades) {
						if (!$this->esTablaVacia($tabla)) {
							$this
								->mensaje("      Registrando sucursal de '$tabla'...");

							$campos = $this->arregloCampos($tabla);
							if (!in_array('Client', $campos) && !in_array('Branch', $campos)) {
								continue;
							}

							$consulta = "
								DELETE FROM `$tabla`
								WHERE IFNULL(Client, 0) = 0 OR IFNULL(Branch, 0) = 0";
							if ($this->consulta($consulta)) {
								$consulta = "
									INSERT INTO lista
									SELECT DISTINCT Client, Branch
									FROM `$tabla`
									ON DUPLICATE KEY UPDATE Client = VALUES(Client), Branch = VALUES(Branch)";
								$this->consulta($consulta);
							}
						} else {
							if (count($registro = $this->buscaRegistro("tablas", "Name", substr($tabla, 0, 6)."%", "OkReporteVacio"))) {
								//$this->error(print_r($registro));
								if ($registro["OkReporteVacio"]) {
									 // ToDo: OkReporteVacio

									$consulta = "
									INSERT INTO lista
									SELECT DISTINCT Client, Branch
									FROM `$tabla`
									ON DUPLICATE KEY UPDATE Client = VALUES(Client), Branch = VALUES(Branch)";
									$this->consulta($consulta);
								}
							}
						}
					}

					$consulta = "
						SELECT LOWER(Name) AS Reporte6,
							LOWER(SUBSTRING(Name, 1, 5)) AS Reporte5,
                            Tipo
						FROM tablas";
					if ($resultado = $this->consulta($consulta)) {
						$tablas = $resultado->fetch_all(MYSQLI_ASSOC);
						$resultado->close();

						foreach ($tablas as $tabla) {
							$consulta = "SHOW TABLES LIKE '{$tabla["Reporte6"]}%'";
							if ($resultado = $this->consulta($consulta)) {
								$reportes = $resultado->fetch_all();
								$resultado->close();

								$encabezados = false;
								if (count($reportes))
									foreach ($reportes as $reporte) {
										if ($reporte[0] != $tabla["Reporte6"]) {
											if (!$encabezados) {
												$encabezados = true;

												$this->mensaje("         '{$tabla["Reporte6"]}'...");
											}

											$this->mensaje("            '{$reporte[0]}'...");
                                            $this->eliminaRegistrosDuplicados($reporte[0]);

											if (!$this->existeTabla($tabla["Reporte6"])) {
												$this->renombraTabla($reporte[0], $tabla["Reporte6"]);
											} else {
                                                $this
                                                    ->copiaRegistros($reporte[0], $tabla["Reporte6"])
                                                    ->eliminaTabla($reporte[0]);
											}

                                            $this->reestructuraTabla($tabla["Reporte6"], $tabla["Reporte6"], $tabla["Tipo"]);
										}
									}
							}
						}
					}

					$this->mensaje("      Consolidando MAC...");
					$reglas = array("filtro" => "mac%");

					if (count($tablas = $this->listaTablas($reglas))) {
						foreach ($tablas as $tabla => $propiedades) {
                            if ($this->existetabla(substr($tabla, 0, 6))) {
                                if (!$this->existeTabla("mac")) {
                                    $this->renombraTabla($tabla, "mac");
                                } else {
                                    $this
                                    ->copiaRegistros($tabla, "mac")
                                    ->eliminaTabla($tabla);
                                }
                            }
                        }

						if ($this->existeTabla("mac")) {
                            if ($this->esTablaVacia("mac")) {
                                $this->mensaje("         Llegaron reportes de MAC pero vienen vacíos");
								$this->eliminaTabla("mac");
							}
                        }
					}
					$this->mensaje();

					$this->mensaje("      Consolidando tablas segmentadas...");
						$this->lanzadorComandos(true, false, "3");

					$this->identificaReportes();

				}

				if ($this->existeArchivo("consolida_despues.txt", SW_SPECS."/")) {

				}
			}
		}

		$this->guardaLog(['02x010330CO', 'Batch 30-CONSOL', null, $this->paquete]);

		return $this;
	}

	protected function identificaReportes() {
		$this->depurar(__METHOD__);

		$this->mensaje("      Identificando reportes a reemplazar (B) y a actualizar (R)");
			if (count($tablas = $this->listaTablas())) {
				$filtro = null;
				foreach ($tablas as $tabla => $propiedades)
					$filtro .= "'$tabla', ";
				$consulta = "
					SELECT LOWER(Name) AS Nombre, Tipo, CampoLlave
					FROM tablas
					WHERE Name IN (" . substr($filtro, 0, -2) . ")";
				if ($resultado = $this->consulta($consulta)) {
					$reportes = $resultado->fetch_all(MYSQLI_ASSOC);
					$resultado->close();

					foreach ($reportes as $reporte) {

						foreach (array(PREFIJO_SND.$this->client, PREFIJO_SIM.$this->client) as $esquema) {

							if (!$this->existeTabla(PREFIJO_SIM.$this->client.".".$reporte["Nombre"])){
								$this
									->mensaje("            No se encontro '".$reporte["Nombre"]."' en la base de datos historica del cliente")
									->mensaje("               Creando estructura para el reporte '".$reporte["Nombre"]."'...");

								$consulta = "
								CREATE TABLE ".PREFIJO_SIM.$this->client.".".$reporte["Nombre"]." LIKE ".PREFIJO_SND.$this->client.".".$reporte["Nombre"];
								if ($this->consulta($consulta)) {
									$this->mensaje("                  Estructura creada para el reporte '".$reporte["Nombre"]."'...");
								}
							} elseif ($this->existeTabla(PREFIJO_SIM.$this->client.".".$reporte["Nombre"])) {
								$consulta = null;

								$this->mensaje("         '{$reporte["Nombre"]}' ({$reporte["Tipo"]})...");

									switch ($reporte["Tipo"]) {
										case "R" :
											$campos = $this->listaCampos($esquema.".".$reporte["Nombre"]);
											if (strpos($campos, "`Date`") !== false)
												$consulta = "DROP `Date`, ";

											if (!$this->existeIndice("_Principal", $esquema.".".$reporte["Nombre"])) {
												$campos_elegidos = "Branch" . (($reporte["CampoLlave"] and $this->existeCampo($reporte["CampoLlave"], $esquema . '.' . $reporte['Nombre'])) ? ", {$reporte["CampoLlave"]}" : "");
												if (in_array($reporte["CampoLlave"], array("Factura", "NumeroOT")))
													if (strpos($campos, "`FechaFactura`") !== false)
														$campos_elegidos .= ", FechaFactura";
												$consulta .= "ADD INDEX _Principal ($campos_elegidos), ";
											}

											break;
										case "S" :
											if ($this->existeCampo('Date', $esquema . '.' . $reporte['Nombre'])) {
												$consultaDrop = 'ALTER TABLE ' . $esquema . '.' . $reporte['Nombre'] . ' DROP COLUMN `Date`';
												$this->consulta($consultaDrop);
											}

											if (!$this->existeIndice("_Principal", $esquema.".".$reporte["Nombre"])) {
												$consultaFecha="SELECT CampoFecha FROM $esquema.tablas WHERE Name='".strtoupper($reporte["Nombre"])."' AND Tipo='S' LIMIT 1";
												if ($resultado = $this->consulta($consultaFecha)) {
													while ($fila = $resultado->fetch_assoc()) {
														$campoFecha = $fila["CampoFecha"];
													}

													if ($this->existeCampo($campoFecha, $esquema . '.' . $reporte['Nombre'])) {
														$consulta = "ADD INDEX _Principal (Branch, $campoFecha), ";
													} else {
														$this->error('No existe el CampoFecha: ' . $campoFecha . ' en ' . $esquema . '.' . $reporte['Nombre'], false);
													}
												}
											}

											break;
										case "B" :
											if (!$this->existeIndice("_Principal", $esquema.".".$reporte["Nombre"]))
												$consulta .= "ADD INDEX _Principal (Branch), ";

											break;
									}

									if ($consulta)
										$this->consulta("ALTER TABLE $esquema.`{$reporte["Nombre"]}` ".substr($consulta, 0, -2));
							}

						}
					}
				}
			}

		return $this;
	}

	protected function formateaMAC() {
		$this->depurar(__METHOD__);

		$this->identificaCliente();
		$this->guardaLog(['01x010350AD', 'Batch 40-FMTMAC', null, $this->paquete]);

		if ($this->client && $this->existeTabla("mac")) {
			if (!$this->existeTabla(PREFIJO_SIM.$this->client.".mac")) {
				$this->mensaje("      Creando MAC...");

				$consulta = "
					CREATE TABLE ".PREFIJO_SIM.$this->client.".mac (
						Client SMALLINT UNSIGNED,
						Branch SMALLINT UNSIGNED,
						`Date` DATE,
						Account VARCHAR(10),
						SubAcc1 VARCHAR(10),
						SubAcc2 VARCHAR(10),
						SubAcc3 VARCHAR(10),
						SubAcc4 VARCHAR(10),
						CostCenter VARCHAR(10),
						Description VARCHAR(50),
						Type CHAR(1),
						Debits DECIMAL(13,4),
						Credits DECIMAL(13,4),
						ClosingBalance DECIMAL(13,4),
						Ref1 CHAR(4),
						Ref2 CHAR(4),
						Ref3 CHAR(4),
						Ref4 CHAR(4),
						Ref5 CHAR(4),
						Ref6 CHAR(4),
						Ref7 CHAR(4),
						INDEX Principal (Client, Branch, Account,
							SubAcc1, SubAcc2, SubAcc3, SubAcc4, CostCenter, Type),
						INDEX Branch (Branch),
						INDEX `Date` (`Date`),
						INDEX Account (Account)
					)";
				$this->consulta($consulta);
			}

			if (!$this->esTablaVacia("mac")) {
				// ToDo
				/*$directorio = CLIENTS . "/" . $this->cliente . "/Scripts";
				if ($this->existeArchivo("fmtmac.php", $directorio)) {
					include_once $directorio . "/fmtmac.php";

					instanciador("FormateaMACCliente")
						->asignaGlobales($self)
						->ejecutar();
				}*/

				$this->procesoExterno('antes', 'fmtmac', '1');

				$this->mensaje("      Reestructurando MAC...");

				$campos = $this->listaCampos("mac");
				if (count(explode(",", $campos)) < 20) {
					$consulta = "
						ALTER TABLE mac
						ADD Type CHAR(1) AFTER Description,
						ADD Ref1 CHAR(4),
						ADD Ref2 CHAR(4),
						ADD Ref3 CHAR(4),
						ADD Ref4 CHAR(4),
						ADD Ref5 CHAR(4),
						ADD Ref6 CHAR(4),
						ADD Ref7 CHAR(4),
						ADD INDEX Principal (Client, Branch, Account,
							SubAcc1, SubAcc2, SubAcc3, SubAcc4, CostCenter, Type),
						ADD INDEX Branch (Branch),
						ADD INDEX `Date` (`Date`),
						ADD INDEX Account (Account)";
					$this->consulta($consulta);
				}

				$this->procesoExterno('despues', 'fmtmac', '1');
			}
		}

		$this->guardaLog(['02x010350AD', 'Batch 40-FMTMAC', null, $this->paquete]);

		return $this;
	}

	protected function actualizaMAC() {
		$this->depurar(__METHOD__);

		$this->identificaCliente();
		$this->mensaje("      Actualizando MAC...");

		$db = PREFIJO_SIM.$this->client;

		if ($this->existeTabla("{$db}.mac")) {
			// Agrega campo AccountChecksum a la Mac
			if (!$this->existeCampo("AccountChecksum","{$db}.mac")) {
				$this->mensaje("	Agregando campo AccountChecksum a la Mac...");
				$consulta = "ALTER TABLE {$db}.mac ADD `AccountChecksum` varchar(32) NULL";
				$this->consulta($consulta);
			}
		}

		if ($this->client && $this->existeTabla("mac") && $this->existeTabla("{$db}.chart")) {
			if (!$this->esTablaVacia("mac")) {
				$consulta = "
					DELETE FROM mac
					WHERE IFNULL(Account, '') = ''";
				$this->consulta($consulta);

				$db = PREFIJO_SIM.$this->client;
				$campos_indice = array("Client", "Branch", "Account", "SubAcc1", "SubAcc2", "SubAcc3", "SubAcc4", "CostCenter", "Type");
				$campos_chart = "`".implode("`,`", $campos_indice)."`";

				$this->mensaje("          Creando temporal de chart...");

				$this->consulta("DROP TABLE IF EXISTS {$db}.`tmp_chart`");
				$this->consulta("DROP TABLE IF EXISTS `tmp_mac`");

				$this->consulta("CREATE TABLE {$db}.tmp_chart LIKE {$db}.chart");

				$this->consulta("CREATE UNIQUE INDEX indtmpchart ON {$db}.tmp_chart ($campos_chart)");

				$this->consulta("INSERT INTO {$db}.tmp_chart SELECT * FROM {$db}.chart GROUP BY $campos_chart");


				$this->mensaje("          Creando temporal de mac...");
				array_push($campos_indice, "Date");
				$campos_mac = "`".implode("`,`", $campos_indice)."`";

				$this->consulta("CREATE TABLE tmp_mac LIKE mac");

				$this->consulta("CREATE UNIQUE INDEX indtmpmac ON tmp_mac ($campos_mac)");

				$this->consulta("INSERT INTO tmp_mac SELECT * FROM mac GROUP BY $campos_mac");

                // Actualizando campos de new_mac y new_char
                $camposUpdate = array(
                    'Account',
                    'SubAcc1',
                    'SubAcc2',
                    'SubAcc3',
                    'SubAcc4',
                    'CostCenter',
                );
                $tablasUpdate = array('`tmp_mac`', $db . '.`tmp_chart`');

                foreach ($tablasUpdate as $tablaUp) {
                    foreach ($camposUpdate as $campoUp) {
                        $consultaUp = '
                            update ignore ' . $tablaUp . ' set ' . $campoUp . ' = "" where ' . $campoUp . ' is null
                        ';
                        $this->consulta($consultaUp);
                    }
                }

				$consulta = "
                    UPDATE tmp_mac AS mac,
						{$db}.tmp_chart AS chart
					SET mac.Type = chart.Type,
						mac.Ref1 = chart.Ref1,
						mac.Ref2 = chart.Ref2,
						mac.Ref3 = chart.Ref3,
						mac.Ref4 = chart.Ref4,
						mac.Ref5 = chart.Ref5,
						mac.Ref6 = chart.Ref6,
						mac.Ref7 = chart.Ref7
					WHERE mac.Client = chart.Client AND mac.Branch = chart.Branch
					AND mac.Account = chart.Account
					AND mac.SubAcc1 = chart.SubAcc1
                    AND mac.SubAcc2 = chart.SubAcc2
					AND mac.SubAcc3 = chart.SubAcc3
                    AND mac.SubAcc4 = chart.SubAcc4
					AND mac.CostCenter = chart.CostCenter
                ";

				if ($this->consulta($consulta)) {
					$this->mensaje("         ".$this->filas_afectadas." registros actualizados");

					$consulta = "
						DELETE m1
						FROM {$db}.mac AS m1,
							(SELECT DISTINCT Branch, `Date`
							FROM tmp_mac) AS m2
						WHERE m1.Branch = m2.Branch AND m1.`Date` = m2.`Date`";
					if ($this->consulta($consulta)) {

						$campos_mig = $this->arregloCampos(PREFIJO_SIM.$this->client.".mac");
						$campos_snd = $this->arregloCampos(PREFIJO_SND.$this->client.".tmp_mac");
						$campos_c = $campos_snd;

						$diferencia = array_udiff($campos_snd, $campos_mig,function($str1,$str2){
							return strcasecmp($str1,$str2);
						});

						if(!empty($diferencia)){
							$remplazar = array_uintersect($campos_mig,$diferencia, function($str1, $str2){
								return strncmp($str1, $str2, 3);
							});

							foreach ($campos_snd as $posicion => $campo) {
								$campos = str_replace($diferencia, $remplazar, $campo);
								$campos_snd[$posicion] = $campos;
							}
						}

						// Volvemos a dejar nulos los registros vacios de tmp_mac, para que no existan en mac nulos y vacios
						foreach ($camposUpdate as $campoUp) {
							$consultaUp = '
								update ignore ' . '`tmp_mac`' . ' set ' . $campoUp . ' = NULL where ' . $campoUp . ' = ""
							';
							$this->consulta($consultaUp);
						}

						$this->mensaje("          Insertamos de tmp_mac a mac...");

						$consulta = "
							INSERT INTO {$db}.mac (".implode(',',$campos_snd).")
							SELECT ".implode(',',$campos_c)."
							FROM tmp_mac";
						if ($this->consulta($consulta)){
							$this->mensaje("         ".$this->filas_afectadas." registros insertados");
						}
					}
				}

				$this->consulta("DROP TABLE IF EXISTS {$db}.`tmp_chart`");
				$this->consulta("DROP TABLE IF EXISTS `tmp_mac`");
			}
		} else {
            $this->mensaje('         No existen las tablas MAC o CHART.');
        }

		$db = PREFIJO_SND.$this->client;

		$this->usarBaseDatos($db);

		return $this;
	}

	protected function actualizaTablas() {
		$this->depurar(__METHOD__);

		$this->identificaCliente();
		$this->guardaLog(['01x010340FM', 'Batch 50-ADDATA', null, $this->paquete]);

		if ($this->client) {
			if ($this->existeArchivo("tablas.txt", SW_SPECS."/")) {

			} else {
				if ($this->existeArchivo("tablas_antes.txt", SW_SPECS."/")) {

				}

				$this->mensaje("      Agregando las tablas del Cliente...");

				if ($this->existeTabla("tablas")) {
					$reglas = array(
						"excluir" => 'tablas,lista,galib,types,mac,chart',
						"renombrar" => "/([a-z]{6})(.)+/");
					if ($tablas = $this->listaTablas($reglas)) {
						$this->mensaje("         Actualizando historico con los datos recibidos...");

						$r_balance = array();
						$consulta = "SELECT `Name` FROM ".PREFIJO_SIM.$this->client.".tablas WHERE Tipo='B'";
						if ($resultado = $this->consulta($consulta)) {
							while ($fila = $resultado->fetch_assoc()) {
								$r_balance[] = strtolower($fila["Name"]);
							}
						}

						foreach ($tablas as $tabla => $propiedades) {
							$this->mensaje("            Actualizando tabla '$tabla'...");

							if ($this->existeTabla(PREFIJO_SIM.$this->client.".$tabla")) {
								if (count($registro = $this->buscaRegistro("tablas", "Name", $tabla, "Tipo,CampoLlave,CampoFecha"))) {
									$tipo = $registro["Tipo"];
									$campo_llave = $registro["CampoLlave"];
									$campo_fecha = $registro["CampoFecha"];

									if($this->historico) {
										if (in_array($tipo, array("R", "S"))) {
											if ($this->existeCampo($campo_fecha, $tabla)) {
												$existeCampoSndReporteS = true;
											} else {
												$this->error('No existe el CampoFecha: ' . $campo_fecha, false);
												$this->mensaje($consulta);
												$existeCampoSndReporteS = false;
											}
										}
									}

									if (!$this->historico) {
										if (in_array($tipo, array("R", "S"))) {
											$existeCampoSndReporteS = true;
											$consulta = "
												DELETE FROM $tabla
												WHERE ($campo_fecha < date_sub(" . date("Ymd", $this->ayer) . ", INTERVAL 62 DAY)
													OR $campo_fecha > " . date("Ymd", $this->ayer) . ")
													OR $campo_fecha IS NULL"
												;

											if ($this->existeCampo($campo_fecha, $tabla)) {
												$this->consulta($consulta);
											} else {
												$this->error('No existe el CampoFecha: ' . $campo_fecha, false);
												$this->mensaje($consulta);
												$existeCampoSndReporteS = false;
											}

											$consulta = "
												DELETE gt
												FROM ".PREFIJO_SIM.$this->client.".gestion AS gt,
												".PREFIJO_SIM.$this->client.".galib AS gl,
												(SELECT DISTINCT Branch FROM $tabla) AS t,
												(SELECT date_sub(".date("Ymd", $this->ayer).", INTERVAL 62 DAY) AS fecha) AS f
												WHERE gt.Ind = gl.Id
													AND gl.Reporte = '$tabla'
													AND gl.Kind <> 'B'
													AND gt.Branch = t.Branch
													AND gt.Ano = year(f.fecha)
													AND gt.Mes = month(f.fecha)
													AND gt.Dia >= day(f.fecha)";
											$this->consulta($consulta);

											$consulta = "
												DELETE gt
												FROM ".PREFIJO_SIM.$this->client.".gestion AS gt,
												".PREFIJO_SIM.$this->client.".galib AS gl,
												(SELECT DISTINCT Branch FROM $tabla) AS t,
												(SELECT date_add(date_sub(".date("Ymd", $this->ayer).", INTERVAL 62 DAY), INTERVAL 1 MONTH) AS fecha) AS f
												WHERE gt.Ind = gl.Id
													AND gl.Reporte = '$tabla'
													AND gl.Kind <> 'B'
													AND gt.Branch = t.Branch
													AND gt.Ano >= year(f.fecha)
													AND gt.Mes >= month(f.fecha)";
											$this->consulta($consulta);

											$consulta = "
												DELETE gt
												FROM ".PREFIJO_SIM.$this->client.".gestion AS gt,
												".PREFIJO_SIM.$this->client.".galib AS gl,
												(SELECT DISTINCT Branch FROM $tabla) AS t,
												(SELECT date_add(date_sub(".date("Ymd", $this->ayer).", INTERVAL 62 DAY), INTERVAL 1 MONTH) AS fecha) AS f
												WHERE gt.Ind = gl.Id
													AND gl.Reporte = '$tabla'
													AND gl.Kind <> 'B'
													AND gt.Branch = t.Branch
													AND gt.Ano > year(f.fecha)";
											$this->consulta($consulta);
										}
									}

									$consulta = null;

									switch ($tipo) {
										case "R" :
											$validaCampoLlave = true;
											$addCampoLlave = false;

											if ($campo_llave and !$this->existeCampo($campo_llave, $tabla)) {
												$consulta = "DESCRIBE ".PREFIJO_SIM.$this->client.".$tabla $campo_llave";
												$resultado = $this->consulta($consulta);
												$consulta = null;

												if ($resultado->num_rows > 0) {
													while ($fila = $resultado->fetch_assoc()) {
														$tipoColumna = $fila["Type"];
														break;
													}
													$resultado->close();

													$consulta = "ALTER TABLE $tabla ADD `$campo_llave` {$tipoColumna}";
													$this->consulta($consulta);
													$consulta = null;
													$addCampoLlave = true;
												}else{
													$validaCampoLlave = false;
													$this->mensaje("               No se pudo actualizar la tabla '$tabla'...");
												}
											}

											if ($campo_llave and !$this->existeCampo($campo_llave, PREFIJO_SIM.$this->client.".$tabla")) {
												$validaCampoLlave = false;
												$this->mensaje("               No se pudo actualizar la tabla '$tabla'...");
											}

											$fecha_factura = null;
											$campos_elegidos = "Branch".($campo_llave ? ", $campo_llave" : "");
											if (in_array($campo_llave, array("Factura", "NumeroOT")))
											/*
												if ($fecha_factura = $this->existeCampo("FechaFactura", $tabla))
													$campos_elegidos .= ", FechaFactura";
											*/
											if ($this->existeCampo("FechaFactura", $tabla)) {
												$campos_elegidos .= ", FechaFactura";
                                                $fecha_factura = true;
											}

											if (addCampoLlave) {
												$this->mensaje("            Actualizando campo " . $campo_llave);

												$consulta =
													"UPDATE ".PREFIJO_SIM.$this->client.".$tabla
													SET $campo_llave = ''
													WHERE $campo_llave IS NULL";
												$this->consulta($consulta);

												$this->mensaje("                registros actualizando ".$this->filas_afectadas);
												$consulta =
													"UPDATE $tabla
													SET $campo_llave = ''
													WHERE $campo_llave IS NULL";
												$this->consulta($consulta);

												$this->mensaje("                registros actualizado".$this->filas_afectadas);
											}

											if ($validaCampoLlave) {
												$consulta = "
													DELETE t1
													FROM ".PREFIJO_SIM.$this->client.".$tabla AS t1,
														(SELECT DISTINCT $campos_elegidos
														FROM $tabla) AS t2
													WHERE t1.Branch = t2.Branch" .
													($campo_llave ? ($addCampoLlave ? (" AND IFNULL(t1.`$campo_llave`, '') = IFNULL(t2.`$campo_llave`, '')") : (" AND t1.`$campo_llave` = t2.`$campo_llave`")) .
														($fecha_factura ? " AND t1.FechaFactura = t2.FechaFactura" : null) : null);
											}

											break;
										case "S" :
											$consulta="SELECT CampoFecha FROM ".PREFIJO_SIM.$this->client.".tablas WHERE Name='".strtoupper($tabla)."' AND Tipo='S'";
											if ($resultado = $this->consulta($consulta)) {
												while ($fila = $resultado->fetch_assoc()) {
													$campoFecha = $fila["CampoFecha"];
												}

												if ($this->existeCampo($campoFecha, PREFIJO_SIM . $this->client . '.' . $tabla) && $existeCampoSndReporteS) {
													$consulta = "
														DELETE t1
														FROM " . PREFIJO_SIM . $this->client . ".$tabla AS t1,
															(SELECT Branch, MIN($campoFecha) AS FechaFactura
															FROM $tabla
															GROUP BY Branch) AS t2
														WHERE t1.Branch = t2.Branch AND t1.$campoFecha >= t2.FechaFactura
													";
												} else {
													$consulta = null;
													$this->error('No se creo la consulta para eliminar datos de ' . PREFIJO_SIM . $this->client . '.' . $tabla . '
													 porque no existe el CampoFecha: ' . $campoFecha, false);
												}
											}

											break;
										case "B" :
											$consulta = "
												DELETE FROM ".PREFIJO_SIM.$this->client.".$tabla
												WHERE Branch IN
													(SELECT DISTINCT Branch
													FROM $tabla)";
									}

									if ($consulta)
										if ($this->consulta($consulta)) {
											$campos_origen = explode(", ", str_replace('`', '', $this->listaCampos($tabla)));
											$campos_destino = explode(", ", str_replace('`', '', $this->listaCampos(PREFIJO_SIM.$this->client.".$tabla")));

											$campos = implode("`, `", array_intersect($campos_origen, $campos_destino));

											$consulta = "
												INSERT IGNORE INTO ".PREFIJO_SIM.$this->client.".$tabla (`$campos`)
												SELECT `$campos`
												FROM $tabla";
											$this->consulta($consulta);
											$this->mensaje("               ".$this->filas_afectadas." registros insertados");
										//	$this->guardaLog(['01x0104PAQU', $tabla, $this->branch, $this->filas_afectadas]);
										}
									// Se forza a que los de tipo B el Date cambie al día de ayer
									$hoy = date("Ymd", $this->hoy);
									$ayer = date("Ymd", $this->ayer);

									switch ($tipo) {
										case 'B':
											$consulta = '
													UPDATE ' . PREFIJO_SIM . $this->client . '.' . $tabla . '
													SET Date = \'' . $ayer . '\'
													WHERE Date = \'' . $hoy . '\'
												';
												$this->consulta($consulta);
												$this->mensaje("               ".$this->filas_afectadas." registros actualizados.");
												$this->guardaLog(['01x0104PAQU', $tabla, $this->branch , $this->filas_afectadas]);
											break;
										case 'S':
											if ($this->existeCampo('Date', PREFIJO_SIM . $this->client . '.' . $tabla)) {
												if (!in_array(strtolower($tabla), $r_balance)) {
													$consulta = '
														UPDATE ' . PREFIJO_SIM . $this->client . '.' . $tabla . '
														SET Date = \'' . $ayer . '\'
														WHERE Branch IN (SELECT DISTINCT Branch FROM '.$tabla.')
													';
													$this->consulta($consulta);
													$this->mensaje("               ".$this->filas_afectadas." registros actualizados.");
													$this->guardaLog(['01x0104PAQU', $tabla, $this->branch , $this->filas_afectadas]);
												}
											}

											break;

										default:
											break;
									}
								} else {
									$this->mensaje("               No se pudo actualizar la tabla '$tabla'...");
								}
							} else {
								$this->clonaTabla($tabla, PREFIJO_SIM.$this->client.".$tabla");
								$this->mensaje("       Clonando Tabla...");
							}
						}
					}
				}

				if ($this->existeArchivo("tablas_despues.txt", SW_SPECS."/")) {

				}
			}
		}

		$this->guardaLog(['02x010340FM', 'Batch 50-ADDATA', null, $this->paquete]);

		return $this;
	}

	protected function procesaPaquete() {
		$this->depurar(__METHOD__);

		$this
			->identificaCliente()
			->mensaje("   Procesando paquete '{$this->paquete}'...")
			->guardaLog(['01x0105PAQU', $this->paquete]);

		$respuestaDescomprime = $this->descomprimePaquete();

		if (!$respuestaDescomprime) {
			$this->guardaLog(['01x0101PAQU', $this->paquete]);
			return $this;
		}

		$this
			->transformaTablas()
			->importaTablas()
			->consolidaTablas()
			->formateaMAC()
			->actualizaMAC()
			->actualizaTablas()
			->procesoExterno('despues','mac','1');

		$this
			->mensaje()
			->mensaje("   Registrando el paquete '{$this->paquete}'...");

		if ($this->existeTabla(PREFIJO_SIM.$this->client . '.docs')) {
			// Inicia código para eliminar duplicados de la tabla docs, debe retirarse en el futuro
			// Antes de crear el INDEX, se eliminarán los registros duplicados para evitar error
            if (!$this->existeCampo("IdRegistro", PREFIJO_SIM . $this->client . ".docs")) {
                $consultaIdRegistro = 'ALTER TABLE ' . PREFIJO_SIM . $this->client . '.docs ADD `IdRegistro` int(10) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST';
                $this->consulta($consultaIdRegistro);
            }

			$consultaIds = '
				SELECT COUNT(IdRegistro) AS conteo, client, branch, date, GROUP_CONCAT(IdRegistro) AS ids
				FROM ' . PREFIJO_SIM . $this->client . '.docs
				GROUP BY client, branch, date
				HAVING COUNT(ids) > 1
			';
			$resultado = $this->consulta($consultaIds);

			if ($resultado) {
				$idsRegistrosEliminar = array();
				$idsRegistrosConservar = array();
				$resultadoDatos = $resultado->fetch_all();
				$resultado->close();

				foreach ($resultadoDatos as $datos) {
					$camposConsulta[] = array(
						'client' => $datos[1],
						'branch' => $datos[2],
						'date' => $datos[3],
					);
					$consultaIdRegistros = '
						SELECT IdRegistro
						FROM ' . PREFIJO_SIM . $this->client . '.docs
						WHERE client = \'' . $datos[1] . '\'
						AND branch = \'' . $datos[2] . '\'
						AND date = \'' . $datos[3] . '\'
					';
					$resultadoIdRegistros = $this->consulta($consultaIdRegistros);

                    if ($resultadoIdRegistros) {
                        $resultadoIds = $resultadoIdRegistros->fetch_all();
						$resultadoIdRegistros->close();
						$idsRegistrosConservar[] = array_shift($resultadoIds[0]);

						// Se crea el arreglo de los IdRegistro que se eliminaran
						foreach ($resultadoIds as $registro) {
							if (!empty($registro[0])) {
								$idsRegistrosEliminar[] = $registro[0];
							}
						}
                    }
				}

				if (!empty($idsRegistrosEliminar)) {
					$this->mensaje('    Eliminando registros duplicados en la tablas docs');
					$consultaDelete = '
						DELETE FROM ' . PREFIJO_SIM . $this->client . '.docs
						WHERE idRegistro IN (' . implode(',', $idsRegistrosEliminar) . ')
					';
					$this->consulta($consultaDelete);
				}
			}
			// Termina código que elimina duplicados, se debe eliminar hasta aquí y dejar el if de abajo

			if (!$this->existeIndice("_Principal", PREFIJO_SIM.$this->client.".docs")) {
				$consulta = "
					ALTER TABLE ".PREFIJO_SIM.$this->client.".docs
					ADD UNIQUE INDEX _Principal (Client, Branch, `Date`)";
				$this->consulta($consulta);
			}
		} else {
			$consulta = "
				CREATE TABLE ".PREFIJO_SIM.$this->client.".docs (
                    `IdRegistro` int(10) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
					Client SMALLINT UNSIGNED,
					Branch SMALLINT UNSIGNED,
					`Date` DATE,
					`Time` TIME,
					Contador SMALLINT UNSIGNED NOT NULL DEFAULT 1,
					UNIQUE INDEX _Principal (Client, Branch, `Date`)
				)";
			$this->consulta($consulta);
		}

		$consulta = "
			INSERT INTO ".PREFIJO_SIM.$this->client.".docs
			SET Client = {$this->client},
				Branch = '{$this->branch}',
				`Date` = ".date("Ymd", $this->fecha_paquete).",
				`Time` = NOW(),
				Contador = 1
			ON DUPLICATE KEY UPDATE Contador = Contador + 1";
		$this->consulta($consulta);

		$this->mensaje();

		return $this;
	}

	protected function validaPaqueteCliente() {
		$this->depurar(__METHOD__);

		$horaValida =  date("H:i:s" ,strtotime("05:00:00"));
		$hora = date("H:i:s");

		$this
			->mensaje("\nValidando paquetes esperados por cliente...")
			->mensaje("  Hora: $hora");

		$paquetes_procesados = count($this->listaArchivos(".zip", SW_TODAY)) + count($this->listaArchivos(".zip", SW_WORKING));

		$this->mensaje("   Paquetes esperados: " . $this->total_paquetes . "  Paquetes procesados : $paquetes_procesados");

		if (($this->total_paquetes == $paquetes_procesados) || ($hora >= $horaValida) ){
			$this
				->mensaje("  Hora: ".date("H:i:s"));
			return true;
		} else {
			$this->mensaje("Esperando llegada de paquetes nuevos...");
			$i=0;
				for ($i=0; $i<600; $i++) {
					if ($i%10==0)
						echo ".";
					sleep(1);
				}
			$this->mensaje();
			return false;
		}

	}

	protected function procesaPaquetes2() {
		$this->depurar(__METHOD__);


		if ($this->client) {
            // Se agrega la preparación de tablas, ya que un cliente nuevo requiere de estas tablas para procesar los paquetes
            $this->preparaTablas();

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

					sleep(3);
					$this->descargaPaquetes();
				}
			} while ($this->validaPaqueteCliente() == false);
		} else {
			$this->procesaPaquete();
		}

		if($this->paquete){
			$this->mensaje("\n   No se encontraron nuevos paquetes");
			$this->mueveArchivo(SW_WORKING."/".$this->paquete, SW_TODAY."/".$this->paquete, false);
			$this->mensaje("      Se concluye el procesamiento de paquetes con el paquete ".$this->paquete."\n");
		}

		$this
			->mensaje("   Sin nuevos paquetes por procesar...")
			->mensaje("      Hora: ".date("H:i:s")."\n");

		return $this;
	}

	protected function NuevosClientesRefvtaMac2char() {
		$this->depurar(__METHOD__);

		$this->decodificaDSN($this->dsn,$servidor, $usuario, $contrasena, $basedatos, $puerto);
		$tempDSN = "mysql://mig$usuario:$contrasena@".DATABASE_SERVER.":5123";

		if ($this->existeTabla(PREFIJO_SIM.$this->client. '.refmos') || $this->existeTabla(PREFIJO_SIM.$this->client. '.refser') || $this->existeTabla(PREFIJO_SIM.$this->client. '.mac')) {
			if ($this->existeTabla(PREFIJO_SIM.$this->client. '.refmos') || $this->existeTabla(PREFIJO_SIM.$this->client. '.refser')) {
				$this->mensaje("Obtenemos, si el cliente se encuetra activo en crm_simetrical para el reporte de refvta");
				
				$refvta = "refvta";
				$consulta = "SELECT v.Made
							FROM crm_simetrical.clients v
							WHERE v.Client =  {$this->client}";
		
				if ($resultado = $this->consulta($consulta)) {
					$fila = $resultado->fetch_assoc();
					$Made = $fila['Made'];
				}
				switch ($Made) {
					case "RCO":
						$refvta = "refvta_rco";
		
						break;
					case "NCL":
					case "NPE":
					case "NAR":
						$refvta = "refvta_nlac";
		
						break;
				}
				
				$consulta = "SELECT pe.Client, pe.proceso, pe.momento, pe.funcion, pe.tipo, pe.orden, pe.estado
							FROM crm_simetrical.procesos_externos pe
							WHERE pe.Client =  {$this->client} AND pe.proceso LIKE '%{$refvta}%'";
				
				if ($resultado = $this->consulta($consulta, $tempDSN)) {
					if (!$fila = $resultado->fetch_assoc()) {
						
						$consulta = "INSERT INTO crm_simetrical.procesos_externos (Client,proceso,momento,funcion,tipo,orden,estado)
									VALUES ({$this->client},'{$refvta}','antes','tablero',1,1,1)";
						if ($this->consulta($consulta))
							$this->mensaje("         {$this->filas_afectadas} registros insertados");

						if($this->consulta($consulta , $tempDSN));
					}
				}
			}
		
			if ($this->existeTabla(PREFIJO_SIM.$this->client. '.mac')) {
				$this->mensaje("Obtenemos, si el cliente se encuetra activo en crm_simetrical para mac2ch2");
		
				$mac2ch2 = "mac2ch2";
				switch (substr($this->client,0,2)) {
					case "48":
						$mac2ch2 = "mac2ch";
						break;
				}
		
				$consulta = "SELECT pe.Client, pe.proceso, pe.momento, pe.funcion, pe.tipo, pe.orden, pe.estado
							FROM crm_simetrical.procesos_externos pe
							WHERE pe.Client =  {$this->client} AND pe.proceso='{$mac2ch2}'";
				
				if ($resultado = $this->consulta($consulta, $tempDSN)) {
					if (!$fila = $resultado->fetch_assoc()) {
						$consulta = "SELECT ce.Client, ce.Branch, ce.Made, ce.Active 
									FROM crm_simetrical.client_extras ce
									where ce.Client = {$this->client}";
				
						if ($resultado = $this->consulta($consulta)) {
							if ($fila = $resultado->fetch_assoc()) {
								
								$consulta = "INSERT INTO crm_simetrical.procesos_externos (Client,proceso,momento,funcion,tipo,orden,estado)
									VALUES ({$this->client},'{$mac2ch2}','despues','mac',1,1,1)";
								if ($this->consulta($consulta))
									$this->mensaje("         {$this->filas_afectadas} registros insertados");
		
								if($this->consulta($consulta , $tempDSN));
							}
						}
					}
				}
			}
		}
		return $this;
	}

	protected function procesaPaquetes() {
		$this->depurar(__METHOD__);


		if ($this->client) {
            // Se agrega la preparación de tablas, ya que un cliente nuevo requiere de estas tablas para procesar los paquetes
            $this->preparaTablas();

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

					sleep(3);
					$this->descargaPaquetes();
				}
			} while (count($paquetes));
		} else {
			$this->procesaPaquete();
		}

		if($this->paquete){
			$this->mensaje("\n   No se encontraron nuevos paquetes");
			$this->mueveArchivo(SW_WORKING."/".$this->paquete, SW_TODAY."/".$this->paquete, false);
			$this->mensaje("      Se concluye el procesamiento de paquetes con el paquete ".$this->paquete."\n");
		}

		$this
			->mensaje("   Sin nuevos paquetes por procesar...")
			->mensaje("      Hora: ".date("H:i:s")."\n");

		return $this;
	}

	protected function indicadoresContables() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x010360CA', 'Batch 60-CALCMC', null, $this->paquete]);

		$this
            ->mensaje("Generando indicadores contables...")
            ->identificaCliente()
            ->usarBaseDatos(PREFIJO_SIM.$this->client, true);

		if ($this->client) {
			if ($this->existeArchivo("contables.txt", SW_SPECS."/")) {

			} else {
                $this->procesoExterno('antes', 'contables', '1');

				if ($this->existeTabla("chart"))
					if (!$this->esTablaVacia("chart") && !$this->esTablaVacia("mac")) {
						$this->mensaje("   Verificando los indices...");

						if (!$this->existeIndice("_Principal", "mac")) {
							$this->mensaje("      Creando el indice de Mac");

							$consulta = "
								ALTER TABLE mac
								ADD INDEX _Principal (Client, Branch, Account, SubAcc1, SubAcc2, SubAcc3, SubAcc4, CostCenter)";
							$this->consulta($consulta);
						}

						if (!$this->existeIndice("_Principal", "chart")) {
							$this->mensaje("      Creando el indice de Chart");

							$consulta = "
								ALTER TABLE chart
								ADD INDEX _Principal (Client, Branch, Account, SubAcc1, SubAcc2, SubAcc3, SubAcc4, CostCenter)";
							$this->consulta($consulta);
						}

						$this->mensaje("   Creando la tabla contables...");

						$this->eliminaTabla("contables");
						$consulta = "
							CREATE TABLE contables (
								Client SMALLINT UNSIGNED,
								Branch SMALLINT UNSIGNED,
								Valor DECIMAL(18,4),
								Ind CHAR(4),
								FechaInd DATE,
								INDEX Ind (Ind)
							)";
						$this->consulta($consulta);

						$this->mensaje("   Obteniendo catalogo de cuentas...");

						$meses14 = date("Y-m-d", strtotime("-14 month", strtotime(date("Y-m-01", $this->ayer))));

						$this->mensaje("   Creamos tabla temporal new_mac para indicadores de balance(Type = B)...");

						$this->consulta("DROP TABLE IF EXISTS new_mac");

						$consulta = "
							CREATE TEMPORARY TABLE new_mac
							SELECT
								*
							FROM mac where Date >='$meses14'and Type = 'R'";
						$this->consulta($consulta);

						$this->consulta("DROP TABLE IF EXISTS new_char");

						$consulta = "
							CREATE TEMPORARY TABLE new_char
							SELECT
								*
							FROM `chart`
							where Client is not null and Branch is not null";
						$this->consulta($consulta);

						// Indexando new_mac y new_char
						$consultaIndexMac = '
							ALTER TABLE new_mac
							ADD INDEX indexMac (client, branch, Account, SubAcc1, SubAcc2, SubAcc3, SubAcc4, CostCenter)
						';
						$this->consulta($consultaIndexMac);

						$consultaIndexChar = '
							ALTER TABLE new_char
							ADD INDEX indexChar (client, branch, Account, SubAcc1, SubAcc2, SubAcc3, SubAcc4, CostCenter)
						';
						$this->consulta($consultaIndexChar);

						// Actualizando campos de new_mac y new_char
						$camposUpdate = array(
							'Account',
							'SubAcc1',
							'SubAcc2',
							'SubAcc3',
							'SubAcc4',
							'CostCenter',
						);
						$tablasUpdate = array('new_mac', 'new_char');

						foreach ($tablasUpdate as $tablaUp) {
							foreach ($camposUpdate as $campoUp) {
								$consultaUp = '
									update ignore `' . $tablaUp . '` set ' . $campoUp . ' = "" where ' . $campoUp . ' is null
								';
								$this->consulta($consultaUp);
							}
						}

						$this->mensaje("   Creamos tabla temporal new_mac2 para indicadores de resultado(Type = R)...");

						$this->consulta("DROP TABLE IF EXISTS new_mac2");

						$consulta = "
							CREATE TEMPORARY TABLE new_mac2
							SELECT
								*
							FROM mac where Date >='$meses14'and Type = 'B'";
						$this->consulta($consulta);

						$this->consulta("DROP TABLE IF EXISTS new_char2");

						$consulta = "
							CREATE TEMPORARY TABLE new_char2
							SELECT
								*
							FROM `chart`
							where Client is not null and Branch is not null";
						$this->consulta($consulta);

						// Indexando new_mac2 y new_char2
						$consultaIndexMac = '
							ALTER TABLE new_mac2
							ADD INDEX indexMac (client, branch, Account, SubAcc1, SubAcc2, SubAcc3, SubAcc4, CostCenter)
						';
						$this->consulta($consultaIndexMac);

						$consultaIndexChar = '
							ALTER TABLE new_char2
							ADD INDEX indexChar (client, branch, Account, SubAcc1, SubAcc2, SubAcc3, SubAcc4, CostCenter)
						';
						$this->consulta($consultaIndexChar);

						$tablasUpdate = array('new_mac2', 'new_char2');

						foreach ($tablasUpdate as $tablaUp) {
							foreach ($camposUpdate as $campoUp) {
								$consultaUp = '
									update ignore `' . $tablaUp . '` set ' . $campoUp . ' = "" where ' . $campoUp . ' is null
								';
								$this->consulta($consultaUp);
							}
						}

						for ($i = 1; $i <= 7; $i++) {
							$this->mensaje("      Procesando columna del Chart # ".$i);

							$campo = "Ref$i";

							$this->mensaje("   Insertando en tabla contables los Type R...");

							$consulta = "
								INSERT INTO contables
								SELECT mac.Client, mac.Branch,
									SUM(mac.Credits) + SUM(mac.Debits) AS Valor,
									cat.$campo AS Ind, mac.`Date` AS FechaInd
								FROM new_mac as mac
								INNER JOIN new_char AS cat
									ON mac.Client = cat.Client
										AND mac.Branch = cat.Branch
                                        AND mac.Account = cat.Account
										AND mac.SubAcc1 = cat.SubAcc1
                                        AND mac.SubAcc2 = cat.SubAcc2
										AND mac.SubAcc3 = cat.SubAcc3
										AND mac.SubAcc4 = cat.SubAcc4
                                        AND mac.CostCenter = cat.CostCenter
								WHERE mac.`Date` >= '$meses14' AND mac.Type = 'R'
									AND cat.Client IS NOT NULL AND cat.Branch IS NOT NULL
									AND cat.$campo IS NOT NULL
								GROUP BY mac.Client, mac.Branch, mac.`Date`, cat.$campo";
							$this->consulta($consulta);

							$this->mensaje("   Insertando en tabla contables los Type B...");

							$consulta = "
								INSERT INTO contables
								SELECT mac.Client, mac.Branch,
									SUM(mac.ClosingBalance) AS Valor,
									cat.$campo AS Ind, mac.`Date` AS FechaInd
								FROM new_mac2 as mac
								INNER JOIN new_char2 AS cat
									ON mac.Client = cat.Client
									AND mac.Branch = cat.Branch
                                    AND mac.Account = cat.Account
									AND mac.SubAcc1 = cat.SubAcc1
                                    AND mac.SubAcc2 = cat.SubAcc2
									AND mac.SubAcc3 = cat.SubAcc3
									AND mac.SubAcc4 = cat.SubAcc4
									AND mac.CostCenter = cat.CostCenter
								WHERE mac.`Date` >= '$meses14' AND mac.Type = 'B'
									AND cat.Client IS NOT NULL AND cat.Branch IS NOT NULL
									AND cat.$campo IS NOT NULL
								GROUP BY mac.Client, mac.Branch, mac.`Date`, cat.$campo";
							$this->consulta($consulta);
						}

						$consulta = "
							CREATE TEMPORARY TABLE temp AS
							SELECT Client, Branch, SUM(Valor) AS Valor, Ind, FechaInd
							FROM contables
							GROUP BY Client, Branch, Ind, FechaInd";
						$this->consulta($consulta);

						$consulta = "DELETE FROM contables";
						$this->consulta($consulta);

						$consulta = "
							INSERT INTO contables
							SELECT *
							FROM temp";
						$this->consulta($consulta);

						$this->eliminaTabla("temp");

						$this->mensaje("   Cambiando los signos...");

						$consulta = "
							UPDATE contables, galib
							SET contables.Valor = -contables.Valor
							WHERE contables.Ind = galib.Id AND galib.AcctSign = -1";
						$this->consulta($consulta);

						$this->mensaje("   Ajustando las fechas...");

						$consulta = "
							UPDATE contables
							SET FechaInd = ".date("Ymd", $this->ayer)."
							WHERE FechaInd > ".date("Ymd", $this->ayer);
						$this->consulta($consulta);
					}

                    $this->procesoExterno('despues', 'contables', '1');
				}

			$this->mensaje();
		}

		$this->guardaLog(['02x010360CA', 'Batch 60-CALCMC', null, $this->paquete]);

		return $this;
	}

	protected function preparaTablas() {
		$this->depurar(__METHOD__);

		$this->guardaLog(['01x0102PTAB', 'Prepara Tablas']);

		$this->decodificaDSN($this->dsn,$servidor, $usuario, $contrasena, $basedatos, $puerto);

		$mysqldump = "mysqldump";
		$mysql = "mysql";

			if (SO == "WINDOWS") {
				$mysqldump = "%mysqldump%";
				$mysql = "%mysql%";
			}

		$this
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("   Preparando las tablas...");

		$this->mensaje("      'galib'...");

		if ($this->existeTabla("galib")) {
			if (!$this->existeIndice("IdKindShared", "galib")) {
				$consulta = "
					ALTER TABLE galib
					ADD INDEX IdKindShared (Id, Kind, Shared)";
				$this->consulta($consulta);
			}
		} elseif ($this->existeArchivo('galib.sql.dump', RAIZ_E . '/' . $this->client . '/')) {
			$this->mensaje("            Importando tabla 'galib'...");

			$directorio=PREFIJO_SIM.$this->client;

			$comando = "$mysql $directorio -h $servidor -P $puerto -u $usuario -p$contrasena --force  < ".RAIZ_E."/".$this->client."/galib.sql.dump";

			$this->ejecutaComando($comando);
		} else {
            // Se copia la tabla 'galib' base
            $this->mensaje("            Importando tabla 'galib' desde 'indice'...");
            $this->clonaTabla('indice.galib-i', PREFIJO_SIM . $this->client . '.galib');
            $this->copiaRegistros('indice.galib-c', PREFIJO_SIM . $this->client . '.galib');

            if (!$this->existeCampo('IdRegistro', PREFIJO_SIM . $this->client . '.galib')) {
                $consultaIdRegistro = 'ALTER TABLE ' . PREFIJO_SIM . $this->client . '.galib ADD `IdRegistro` int(10) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST';
                $this->consulta($consultaIdRegistro);
            }
        }

		$this->mensaje("      'gestion'...");

		if ($this->existeTabla("gestion")) {
			$this->limpiaDuplicados('gestion', 'Branch, Ano, Mes, Dia, Ind');

			if (!$this->existeIndice("_Principal", "gestion")) {
				$consulta = "
					ALTER TABLE gestion
					ADD UNIQUE INDEX _Principal (Branch, Ano, Mes, Dia, Ind)";
				$this->consulta($consulta);
			}

			if (!$this->existeIndice("AnoMesInd", "gestion")) {
				$consulta = "
					ALTER TABLE gestion
					ADD INDEX AnoMesInd (Ano, Mes, Ind)";
				$this->consulta($consulta);
			}

			if (!$this->existeIndice("IndValorSaldo", "gestion")) {
				$consulta = "
					ALTER TABLE gestion
					ADD INDEX IndValorSaldo (Ind, Valor, Saldo)";
				$this->consulta($consulta);
			}

			if (!$this->existeIndice("DiaIndBranchAnoMes", "gestion")) {
				$consulta = "
					ALTER TABLE gestion
					ADD INDEX DiaIndBranchAnoMes (Dia, Ind, Branch, Ano, Mes)";
				$this->consulta($consulta);
			}

            if (!$this->existeIndice('BranchAnoMesIndValor', 'gestion')) {
                $consulta = '
                    ALTER TABLE gestion
                    ADD INDEX BranchAnoMesIndValor (Branch, Ano, Mes, Ind, Valor)
                ';
                $this->consulta($consulta);
            }

			if ($this->existeCampo("Valor", "gestion")) {
				$consulta = "
					ALTER TABLE gestion
					MODIFY COLUMN Valor DECIMAL(35,4);
				";
				$this->consulta($consulta);
			}

			if ($this->existeCampo("Saldo", "gestion")) {
				$consulta = "
					ALTER TABLE gestion
					MODIFY COLUMN Saldo DECIMAL(35,4);
				";
				$this->consulta($consulta);
			}
		} else {
			$consulta = "
				CREATE TABLE gestion (
					Client SMALLINT UNSIGNED,
					Branch SMALLINT UNSIGNED,
					Ind CHAR(4),
					Ano SMALLINT UNSIGNED,
					Mes SMALLINT UNSIGNED,
					Dia SMALLINT UNSIGNED,
					Valor DECIMAL(35,4),
					Saldo DECIMAL(35,4),
					UNIQUE INDEX _Principal (Branch, Ano, Mes, Dia, Ind),
					INDEX AnoMesInd (Ano, Mes, Ind),
					INDEX IndValorSaldo (Ind, Valor, Saldo),
					INDEX DiaIndBranchAnoMes (Dia, Ind, Branch, Ano, Mes)
				)";
			$this->consulta($consulta);
		}

		$this->mensaje("      'tablas'...");

		if (!$this->existeTabla("tablas"))
			$this->clonaTabla("indice.auto-tbl", "tablas");

		$this->mensaje();

		$this->guardaLog(['02x0102PTAB', 'Prepara Tablas']);

		return $this;
	}

	protected function limpiaDuplicados($tabla = null, $campos = null) {
        $this->depurar(__METHOD__);

		if (empty($tabla) or empty($campos)) {
			return $this;
		}

        $consulta = "
			SELECT $campos
			FROM $tabla
			GROUP BY $campos
			HAVING COUNT(*) > 1";
		if ($resultado = $this->consulta($consulta)) {
			if($resultado->num_rows > 0){
				$this->eliminaTabla("temp");

				$consulta = "
					CREATE TEMPORARY TABLE temp AS
					SELECT *
					FROM $tabla
					GROUP BY $campos
				";
				$this->consulta($consulta);

				$consulta = "TRUNCATE TABLE $tabla";
				$this->consulta($consulta);

				$consulta = "
					INSERT INTO $tabla
					SELECT *
					FROM temp
				";
				$this->consulta($consulta);
			}
		}

        return $this;
    }

	/**
	 * Método que elimina registros que tengan un valor distinto en su campo Client al
	 * cliente que se está procesando. Hace una búsqueda de todas las tablas, verificando la
	 * existencia del campo Client.
	 */
	protected function limpiaDatosOtrosClientes() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102LDOC', 'limpia Datos de Otros Clientes']);

		$this->mensaje('   Buscando datos de otros clientes en: ' . PREFIJO_SIM . $this->client);
		$baseDatos = $this->baseDatosActual();
		$this->usarBaseDatos(PREFIJO_SIM . $this->client, true);

		$tablas = array_keys($this->listaTablas());

        if (!empty($tablas)) {
            foreach ($tablas as $tabla) {
                if ($this->existeCampo('Client', $tabla)) {
                    $this->mensaje('   ' . $tabla);

                    $consulta = '
						SELECT DISTINCT Client
						FROM `' . $tabla . '`
						WHERE Client <> ' . $this->client . '
					';

                    $resultado = $this->consulta($consulta);
                    $otrosClientes = $resultado->fetch_all();

                    if (!empty($otrosClientes)) {
                        $this->mensaje('      Se encontraron datos de otros clientes en: ' . $tabla);
						$otrosClientes = array_column($otrosClientes, 0);

                        foreach ($otrosClientes as $otroCliente) {
                            $consultaDelete = '
								DELETE FROM ' . $tabla . '
								WHERE Client = ' . $otroCliente . '
							';

							$resultadoDelete = $this->consulta($consultaDelete);

                            if ($resultadoDelete) {
                                $this->mensaje('         Se eliminaron datos del cliente ' . $otroCliente . ' en ' . $tabla);
                            }
                        }
                    }
                }
            }
        }

		$this->usarBaseDatos($baseDatos);

		$this->guardaLog(['02x0102LDOC', 'Limpia Datos de Otros Clientes']);

		return $this;
	}

	protected function verificaIndicadores() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102VIND', 'Verificador de indicadores']);

		$this
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("   Homologando indicadores...");

		// ToDo: Sólo exise una tabla de indicadores base

		$this->mensaje("   Validando indicadores unicos...");

		$consulta = "
			SELECT Id, count(*) AS Cantidad
			FROM galib
			GROUP BY Id
			HAVING Cantidad > 1";
		if ($resultado = $this->consulta($consulta)) {
			$duplicados = array();
			while ($fila = $resultado->fetch_assoc())
				$duplicados[] = $fila["Id"];
			$resultado->close();

			if (count($duplicados)){
				$this->error("Los indicadores ".implode(", ", $duplicados)." se encuentran duplicados", false);
				$this->guardaLog(['01x0101EIND', 'Los indicadores ' . implode(', ', $duplicados) . ' se encuentran duplicados']);
				$this->deshabilitakpi(implode(", ", $duplicados));
			}
		}

		$this->mensaje("   Deshabilitando los indicadores con Type y sin reporte...");

		$consulta = "
			UPDATE galib
			SET Estimatecalcvalue = 1
			WHERE ifnull(Reporte, '') = ''
				AND ifnull(Campotype, '') <> ''";
		$this->consulta($consulta);

		$this->mensaje("   Verificando los indicadores...");

		$indicadores = null;
		$consulta = "
			SELECT *
			FROM galib
			WHERE ifnull(Reporte, '') <> ''
				AND ifnull(Campofecha, '') <> ''
				AND ifnull(Estimatecalcvalue, 0) = 0";
		if ($resultado = $this->consulta($consulta)) {
			$indicadores = count($resultado->fetch_all());
			$resultado->close();
		}

		if ($indicadores) {
			$this->mensaje("      Existen indicadores");

			$consulta = null;
			$campos = array("Campofecha", "Campoclave");
			for ($i = 1; $i <= 5; $i++)
				$campos[] = "Campofiltro".$i;

			foreach ($campos as $campo)
				$consulta .= "
					(SELECT DISTINCT Reporte, $campo AS Campo
					FROM galib
					WHERE ifnull(Reporte, '') <> ''
						AND ifnull($campo, '') <> ''
						AND ifnull(Estimatecalcvalue, 0) = 0)
					UNION ALL";
			$consulta = "
				SELECT DISTINCT Reporte, Campo
				FROM (
					".substr($consulta, 0, -10)."
				) AS t
				WHERE Campo <> 'Date'
					AND Campo NOT LIKE '%\_'";
			if ($resultado = $this->consulta($consulta)) {
				$reportes = array();
				while ($fila = $resultado->fetch_assoc())
					$reportes[strtolower($fila["Reporte"])][] = $fila["Campo"];
				$resultado->close();

				$this->depurar(print_r($reportes, true));

				if (count($reportes)) {
					foreach ($reportes as $reporte => $campos) {
						if ($this->existeTabla($reporte)) {
							$campos_origen = $this->arregloCampos($reporte);
							$this->depurar(print_r($campos_origen, true));

							if (count($no_existentes = array_diff($campos, $campos_origen))){
								$this->error("'".implode("', '", $no_existentes)."' no existe en la tabla '$reporte'", false);
								$this->guardaLog(['01x0101INDI', "".implode(", ", $no_existentes)." no existe en la tabla $reporte"]);
							}
						} else {
							$this->error("La tabla '$reporte' no existe", false);
							$this->guardaLog(['01x0101INDI', 'La tabla ' . $reporte . ' configurada en el galib no existe']);
						}
					}

					$this->mensaje("      Existen todos los campos en las tablas");
				}
			}

			$formulas = array();

			for ($i = 1; $i <= 12; $i++) {
				$consulta = "
					SELECT DISTINCT For$i AS Ind
					FROM galib
					WHERE ifnull(Estimatecalcvalue, 0) = 0
					HAVING Ind LIKE 'I%' OR Ind LIKE 'C%'";
				if ($resultado = $this->consulta($consulta)) {
					while ($fila = $resultado->fetch_assoc())
						$formulas[$fila["Ind"]] = true;
					$resultado->close();
				}
			}

			$formulas = array_keys($formulas);
			$this->depurar(print_r($formulas, true));

			$encontrados = array();

			$consulta = "
				SELECT Id
				FROM galib
				WHERE ifnull(Estimatecalcvalue, 0) = 0
					AND Id IN ('".implode("', '", $formulas)."')";
			if ($resultado = $this->consulta($consulta)) {
				while ($fila = $resultado->fetch_assoc())
					$encontrados[] = $fila["Id"];
				$resultado->close();
			}

			if (count($no_encontrados = array_diff($formulas, $encontrados))) {
				$this->error("Los indicadores '".implode("', '", $no_encontrados)."' configurados en las formulas no existen", false);
				$this->guardaLog(['01x0101NIND', 'Los indicadores ' . implode(',', $no_encontrados) . ' configurados en las formulas no existen']);
			}

			$this
				->mensaje("      Existen todos los indicadores de las formulas")
				->mensaje()
				->mensaje("   Validando datos de otros clientes...");

			unset($reportes["firsvp"]);
			$reportes = array_merge(array_keys($reportes), array("galib", "gestion"));

			foreach ($reportes as $reporte) {
				if ($this->existeTabla($reporte)) {
					if ($this->existeCampo("Client", $reporte)) {
						$this->mensaje("      '$reporte'...");

						$consulta = "
								SELECT DISTINCT Client
								FROM $reporte
								WHERE Client <> {$this->client}
						";

						if ($resultado = $this->consulta($consulta)) {
							while ($fila = $resultado->fetch_assoc()) {
								if ($client = $fila["Client"]) {
									$this->error("Existen datos del cliente " . $client . " en " . $reporte, false);

									$consultaDelete = "
											DELETE FROM $reporte
											WHERE Client = $client
									";

									$resultadoDelete = $this->consulta($consultaDelete);

									if ($resultadoDelete) {
											$this->mensaje("Se eliminaron datos del cliente '$client' en el '$reporte'");
									}
								}
							}

							$resultado->close();
						}
					}
				}
			}
		}

		$this->mensaje();

		$this->guardaLog(['02x0102VIND', 'Verificador de indicadores']);

		return $this;
	}

    protected function limpiaTypes() {
        $this->depurar(__METHOD__);
        $this->eliminaTabla("temp");

        $consulta = '
            CREATE TEMPORARY TABLE temp AS
            SELECT Reporte, Campo, TRIM(Tipo), Branch, MAX(Type) AS Type
            FROM types
            GROUP BY Reporte, Campo, TRIM(Tipo), Branch
        ';
        $this->consulta($consulta);

        $consulta = 'DELETE FROM types';
        $this->consulta($consulta);

        $consulta = '
            INSERT INTO types (Reporte, Campo, Tipo, Branch, Type)
            SELECT *
            FROM temp
        ';
        $this->consulta($consulta);

        if (!$this->existeIndice("_Principal", "types")) {
            $consulta = '
                ALTER TABLE types
                ADD UNIQUE INDEX _Principal (Reporte, Campo, Tipo, Branch)
            ';
            $this->consulta($consulta);
        }

        return $this;
    }

	protected function actualizaTypes() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['02x0102ATYP', 'Actualizacion de Types']);

		$this
			->usarBaseDatos(PREFIJO_SIM . $this->client, true)
			->mensaje('   Actualizando los types...');

		$consulta = null;

		if ($this->existeTabla("types")) {
			if (!$this->existeIndice("_Principal", "types")) {
				$this->limpiaTypes();
			}
		} else {
			$consulta = '
				CREATE TABLE types (
					Reporte VARCHAR(8),
					Campo VARCHAR(30),
					Tipo VARCHAR(50),
					Branch SMALLINT UNSIGNED,
					Type CHAR(3),
					UNIQUE INDEX _Principal (Reporte, Campo, Tipo, Branch)
			)';
            $this->consulta($consulta);
		}

		$types = array();

		$consulta = "
			SELECT DISTINCT Reporte, Campotype
			FROM galib
			WHERE ifnull(Reporte, '') <> ''
				AND ifnull(Campotype, '') <> ''
				AND ifnull(Estimatecalcvalue, 0) = 0";
		if ($resultado = $this->consulta($consulta)) {
			while ($fila = $resultado->fetch_assoc())
				$types[strtolower($fila["Reporte"])][] = $fila["Campotype"];
			$resultado->close();

			$this->depurar(print_r($types, true));

			$consulta = null;
			foreach ($types as $reporte => $campos)
				if ($this->existeTabla($reporte))
					foreach ($campos as $campo)
						if($this->existeCampo($campo, $reporte)) {
							$consulta .= "
								(SELECT DISTINCT '$reporte' AS Reporte,
									'$campo' AS Campo, $campo AS Tipo, Branch
								FROM $reporte)
								UNION ALL";
						}

			if ($consulta) {
				$consulta = "
					INSERT INTO types (Reporte, Campo, Tipo, Branch)
					SELECT UPPER(Reporte), Campo, Tipo, Branch
					FROM (
						".substr($consulta, 0, -10)."
					) AS t
					ON DUPLICATE KEY UPDATE Tipo = VALUES(Tipo)";
				if ($this->consulta($consulta))
					$this->mensaje("      ".$this->filas_afectadas." Types detectados");
			}

			$consulta = "
				DELETE FROM types
				WHERE ifnull(Branch, 0) = 0";
			$this->consulta($consulta);

            $this->limpiaTypes();

			$consulta = "
				UPDATE types
				SET Tipo = TRIM(Tipo)";
			$this->consulta($consulta);
		}

		$this->mensaje(" Actualizamos los Types con el Tipo que ya existe en la tabla para reducir los Nullos ...");

		if ($this->existeTabla("temp_types")){
			$this->eliminaTabla("temp_types");
		}

		$consulta = "CREATE TABLE temp_types
			SELECT
				Reporte, Campo, Tipo, Type, COUNT(DISTINCT Type) AS TypeCount
			FROM types
			GROUP BY Reporte , Campo , Tipo
			HAVING TypeCount = 1
			";
		$this->consulta($consulta);

		$consulta = "UPDATE types AS t
			JOIN temp_types AS tt
			ON t.Reporte = tt.Reporte
			AND t.Campo = tt.Campo
			AND (t.Tipo = tt.Tipo OR (t.Tipo IS NULL AND tt.Tipo IS NULL))
			SET t.Type = tt.Type
			WHERE t.Type IS NULL";
		if ($this->consulta($consulta)) {
			$this->mensaje("   ".$this->filas_afectadas." Types actualizados");
		}

		$this->eliminaTabla("temp_types");

		$this->mensaje();

		$this->guardaLog(['02x0102ATYP', 'Actualizacion de Types']);
		return $this;
	}

	protected function creaTablasParametro() {
		$this->depurar(__METHOD__);

		$this->identificaCliente();

		$this
			->usarBaseDatos(PREFIJO_SC.$this->client, true)
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("      Creando las tablas parametro...");

		$this
			->mensaje("         Creando _{$this->client}...")
			->eliminaTabla(PREFIJO_SC.$this->client."._".$this->client);

		$consulta = "
			CREATE TABLE ".PREFIJO_SC.$this->client."._{$this->client} AS
			SELECT {$this->client} AS Client";
		$this->consulta($consulta);

		$this
			->mensaje("         Creando GAClient...")
			->eliminaTabla(PREFIJO_SC.$this->client.".gaclient");

		$consulta = "
			CREATE TABLE ".PREFIJO_SC.$this->client.".gaclient AS
			SELECT Client, Branch, ClientID, Name, Made, LongName, Division, Pais, Orden
			FROM crm_simetrical.clients
			WHERE Client = {$this->client}
				AND ifnull(Active, '') = ''";
		$this->consulta($consulta);

		$this
			->mensaje("         Creando GABranch...")
			->eliminaTabla(PREFIJO_SC.$this->client.".gabranch");

		$consulta = "
			CREATE TABLE ".PREFIJO_SC.$this->client.".gabranch AS
			SELECT Client, Name, Branch, Made, Active
			FROM crm_simetrical.clients
			WHERE Client = {$this->client}
				AND ifnull(Active, '') = ''";
		$this->consulta($consulta);

		$this
			->mensaje("         Creando Tablas...")
			->eliminaTabla(PREFIJO_SC.$this->client.".tablas");

		if (!$this->existeTabla(PREFIJO_SIM.$this->client.".tablas")) {
			$this->guardaLog(['01x0101DBTB', 'no existe la tabla tablas']);
			$this->eliminaArchivo(TEMP."/sincronizado.txt", null);
		}

		$this->clonaTabla(PREFIJO_SIM.$this->client.".tablas",
			PREFIJO_SC.$this->client.".tablas");

		if ($this->existeCampo("IdRegistro", PREFIJO_SC.$this->client.".tablas")) {
			$consulta = "
				ALTER TABLE ".PREFIJO_SC.$this->client.".tablas
				DROP IdRegistro";
			$this->consulta($consulta);
		}

		$this
			->mensaje("         Creando Types...")
			->eliminaTabla(PREFIJO_SC.$this->client.".types");

		$consulta = "
            CREATE TABLE " . PREFIJO_SC . $this->client . ".types AS
            SELECT t.Reporte, t.Campo, t.Tipo, t.Branch, MAX(t.Type) AS Type, c.Name
            FROM types t
            LEFT JOIN crm_simetrical.clients c on c.branch = t.Branch and c.Client = " . $this->client . "
            WHERE t.Branch IN (".implode(", ", array_keys($this->sucursales)).")
            GROUP BY t.Reporte, t.Campo, t.Tipo, t.Branch";
		$this->consulta($consulta);

		$consulta = "
			ALTER TABLE ".PREFIJO_SC.$this->client.".types
			ADD UNIQUE INDEX _Principal (Reporte, Campo, Tipo, Branch)";
		$this->consulta($consulta);

		$this
			->mensaje("         Creando GAFiles...")
			->eliminaTabla(PREFIJO_SC.$this->client.".gafiles");

		$consulta = "
			CREATE TABLE ".PREFIJO_SC.$this->client.".gafiles (
				Detalles VARCHAR(30),
				Reporte VARCHAR(8),
				Dept SMALLINT UNSIGNED,
				DataF VARCHAR(8),
				RowF1 VARCHAR(8),
				RowF2 VARCHAR(8),
				ColumnF1 VARCHAR(8),
				ColumnF2 VARCHAR(8),
				HeaderF1 VARCHAR(8),
				HeaderF2 VARCHAR(8),
				UNIQUE INDEX _Principal(Reporte)
			)";
		$this->consulta($consulta);

		$consulta = "
			INSERT INTO ".PREFIJO_SC.$this->client.".gafiles
				(Detalles, Reporte, Dept)
			SELECT t.Descripcion, g.Reporte, t.Dept
			FROM galib AS g
			INNER JOIN tablas AS t
				ON g.Reporte = t.Name
			WHERE ifnull(g.Reporte, '') <> ''
				AND ifnull(g.Estimatecalcvalue, 0) >= 0
			GROUP BY g.Reporte";
		$this->consulta($consulta);

		$this
			->mensaje("         Creando GALib...")
			->eliminaTabla(PREFIJO_SC.$this->client.".galib");

		$this->clonaTabla(PREFIJO_SIM.$this->client.".galib",
			PREFIJO_SC.$this->client.".galib");

		if ($this->existeCampo("IdRegistro", PREFIJO_SC.$this->client.".galib")) {
			$consulta = "
				ALTER TABLE ".PREFIJO_SC.$this->client.".galib
				DROP IdRegistro";
			$this->consulta($consulta);
		}

		$this->mensaje("         Creando GALabels...");

        if (!$this->existeTabla(PREFIJO_SIM . $this->client . '.galabels')) {
            $this->mensaje("            Importando tabla 'galabels' desde 'indice'...");
            $this->clonaTabla('indice.galabels', PREFIJO_SIM . $this->client . '.galabels');
        }

		if ($this->existeCampo('IdRegistro', PREFIJO_SIM . $this->client . '.galabels')) {
			$consulta = '
                ALTER TABLE ' . PREFIJO_SIM . $this->client . '.galabels
				DROP IdRegistro
            ';
			$this->consulta($consulta);
		}

		$consulta = '
			UPDATE ' . PREFIJO_SIM . $this->client . '.galabels
			SET Mexico = ' . $this->client . ',
            Ingles = ' . $this->client . '
			WHERE Indice = 200
        ';
		$this->consulta($consulta);

        $consulta = '
			UPDATE ' . PREFIJO_SIM . $this->client . '.galabels
			SET Mexico = (
                SELECT name FROM crm_simetrical.groups where client = ' . $this->client . '
            ),
            Ingles = (
                SELECT name FROM crm_simetrical.groups where client = ' . $this->client . '
            )
			WHERE Indice = 131
        ';
		$this->consulta($consulta);

        $this->eliminaTabla(PREFIJO_SC . $this->client . '.galabels');
        $this->clonaTabla(PREFIJO_SIM . $this->client . '.galabels', PREFIJO_SC . $this->client . '.galabels');

		$this
			->mensaje("         Creando GASchema...")
			->eliminaTabla(PREFIJO_SC.$this->client.".gaschema");

		if (!$this->existeTabla(PREFIJO_SIM.$this->client.".schema")) {
			$this->guardaLog(['01x0101DBTB', 'no existen la tabla schema']);
			$this->eliminaArchivo(TEMP."/sincronizado.txt", null);

            $consulta = "
                CREATE TABLE " . PREFIJO_SIM . $this->client . ".`schema` (
                    `IdRegistro` int(10) unsigned NOT NULL AUTO_INCREMENT,
                    `Branch` smallint(5) unsigned DEFAULT NULL,
                    `Reporte` varchar(8) DEFAULT NULL,
                    `Campo` varchar(25) DEFAULT NULL,
                    `Tipo` char(5) DEFAULT NULL,
                    `Count` smallint(6) DEFAULT NULL,
                    `Requerido` smallint(6) DEFAULT NULL,
                    `Orden` smallint(6) DEFAULT NULL,
                    PRIMARY KEY (`IdRegistro`)
                )
            ";
            $this->consulta($consulta);
		}

		$this
			->clonaTabla(PREFIJO_SIM.$this->client.".schema",
			PREFIJO_SC.$this->client.".gaschema");

		if ($this->existeCampo("IdRegistro", PREFIJO_SC.$this->client.".gaschema")) {
			$consulta = "
				ALTER TABLE ".PREFIJO_SC.$this->client.".gaschema
				DROP IdRegistro";
			$this->consulta($consulta);
		}

		$this
			->mensaje("         Creando GAObj...")
			->eliminaTabla(PREFIJO_SC.$this->client.".gaobj");

        if (!$this->existeTabla(PREFIJO_SIM . $this->client . '.objetivo')) {
            $consulta = "
                CREATE TABLE " . PREFIJO_SIM . $this->client . ".objetivo (
                    `IdRegistro` int(10) unsigned NOT NULL AUTO_INCREMENT,
                    `Client` smallint(5) unsigned DEFAULT NULL,
                    `Branch` smallint(5) unsigned DEFAULT NULL,
                    `Id` char(5) DEFAULT NULL,
                    `Enero` decimal(15,4) DEFAULT NULL,
                    `Febrero` decimal(15,4) DEFAULT NULL,
                    `Marzo` decimal(15,4) DEFAULT NULL,
                    `Abril` decimal(15,4) DEFAULT NULL,
                    `Mayo` decimal(15,4) DEFAULT NULL,
                    `Junio` decimal(15,4) DEFAULT NULL,
                    `Julio` decimal(15,4) DEFAULT NULL,
                    `Agosto` decimal(15,4) DEFAULT NULL,
                    `Septiembre` decimal(15,4) DEFAULT NULL,
                    `Octubre` decimal(15,4) DEFAULT NULL,
                    `Noviembre` decimal(15,4) DEFAULT NULL,
                    `Diciembre` decimal(15,4) DEFAULT NULL,
                    PRIMARY KEY (`IdRegistro`)
              )
            ";
            $this->consulta($consulta);
        }

		$consulta = "
			CREATE TABLE ".PREFIJO_SC.$this->client.".gaobj (
				Branch SMALLINT UNSIGNED,
				Ind CHAR(4),
				Mes SMALLINT UNSIGNED,
				Valor DECIMAL(13,4),
				UNIQUE INDEX _Principal (Branch, Ind, Mes)
			)";
		$this->consulta($consulta);

		$sucursales = null;
		foreach ($this->sucursales as $branch => $parametros)
			$sucursales .= "
				(SELECT $branch AS Branch)
				UNION ALL";

		$meses = null;
		for ($i = 1; $i <= 12; $i++)
			$meses .= "
				(SELECT $i AS Mes)
				UNION ALL";

		$consulta = "
			INSERT INTO ".PREFIJO_SC.$this->client.".gaobj
			SELECT distinct b.Branch, g.Id, m.Mes, 0
			FROM galib AS g
			CROSS JOIN (
				".substr($sucursales, 0, -10)."
			) AS b
			CROSS JOIN (
				".substr($meses, 0, -10)."
			) AS m
			WHERE  g.Id LIKE 'I%' OR g.Id LIKE 'C%' AND  ifnull(g.Estimatecalcvalue, 0) = 0 ";
		$this->consulta($consulta);

		$consulta = "
			DELETE FROM objetivo
			WHERE ifnull(Enero, 0) = 0
				AND ifnull(Febrero, 0) = 0
				AND ifnull(Marzo, 0) = 0
				AND ifnull(Abril, 0) = 0
				AND ifnull(Mayo, 0) = 0
				AND ifnull(Junio, 0) = 0
				AND ifnull(Julio, 0) = 0
				AND ifnull(Agosto, 0) = 0
				AND ifnull(Septiembre, 0) = 0
				AND ifnull(Octubre, 0) = 0
				AND ifnull(Noviembre, 0) = 0
				AND ifnull(Diciembre, 0) = 0";
		$this->consulta($consulta);

		$consulta = null;
		foreach($this->meses_largos as $mes => $nombre)
			if ($mes)
				$consulta .= "
					(SELECT Branch, Id, $mes, $nombre
					FROM objetivo
					WHERE Branch IN (".implode(", ", array_keys($this->sucursales))."))
					UNION ALL";

		$consulta = "
			INSERT INTO ".PREFIJO_SC.$this->client.".gaobj
			SELECT *
			FROM (
				".substr($consulta, 0, -10)."
			) AS o
			ON DUPLICATE KEY UPDATE Valor = VALUES(Valor)";
		$this->consulta($consulta);

		$this
			->mensaje("         Creando GAMfr...")
			->eliminaTabla(PREFIJO_SC.$this->client.".gamfr");

		$consulta = "
			CREATE TABLE ".PREFIJO_SC.$this->client.".gamfr (
				Branch SMALLINT UNSIGNED,
				Ind CHAR(4),
				Mes SMALLINT UNSIGNED,
				Valor DECIMAL(13,4),
				UNIQUE INDEX _Principal (Branch, Ind, Mes)
			)";
		$this->consulta($consulta);

		if ($this->existeTabla("fabrica")) {
			$consulta = "
				DELETE FROM fabrica
				WHERE ifnull(Enero, 0) = 0
					AND ifnull(Febrero, 0) = 0
					AND ifnull(Marzo, 0) = 0
					AND ifnull(Abril, 0) = 0
					AND ifnull(Mayo, 0) = 0
					AND ifnull(Junio, 0) = 0
					AND ifnull(Julio, 0) = 0
					AND ifnull(Agosto, 0) = 0
					AND ifnull(Septiembre, 0) = 0
					AND ifnull(Octubre, 0) = 0
					AND ifnull(Noviembre, 0) = 0
					AND ifnull(Diciembre, 0) = 0";
			$this->consulta($consulta);

			$consulta = "
				INSERT INTO ".PREFIJO_SC.$this->client.".gamfr
				SELECT distinct b.Branch, g.Id, m.Mes, 0
				FROM galib AS g
				CROSS JOIN (
					".substr($sucursales, 0, -10)."
				) AS b
				CROSS JOIN (
					".substr($meses, 0, -10)."
				) AS m
				WHERE g.Id LIKE 'I%' OR g.Id LIKE 'C%' AND  ifnull(g.Estimatecalcvalue, 0) = 0";
			$this->consulta($consulta);

			$consulta = null;
			foreach($this->meses_largos as $mes => $nombre)
				if ($mes)
					$consulta .= "
						(SELECT Branch, Id, $mes, $nombre
						FROM fabrica
						WHERE Branch IN (".implode(", ", array_keys($this->sucursales))."))
						UNION ALL";

			$consulta = "
				INSERT INTO ".PREFIJO_SC.$this->client.".gamfr
				SELECT *
				FROM (
					".substr($consulta, 0, -10)."
				) AS o
				ON DUPLICATE KEY UPDATE Valor = VALUES(Valor)";
			$this->consulta($consulta);
		}

		$this
			->mensaje("         Creando GAPoints...")
			->eliminaTabla(PREFIJO_SC.$this->client.".gapoints");

		$consulta = "
			CREATE TABLE ".PREFIJO_SC.$this->client.".gapoints (
			`Branch` SMALLINT(5) ,
			`Ind` CHAR(4),
			`D#` DECIMAL(15,4),
			`D%` DECIMAL(15,14),
			`I#` DECIMAL(15,4),
			`I%` DECIMAL(15,4),
			`Pos` DECIMAL(15,4),
			`Neg` DECIMAL(15,4),
			`Points` DECIMAL(15,4))";
		$this->consulta($consulta);

		$this
			->mensaje("         Creando IndMfr...")
			->eliminaTabla(PREFIJO_SC.$this->client.".indmfr");

		$consulta = "
			CREATE TABLE ".PREFIJO_SC.$this->client.".indmfr AS
			SELECT Id
			FROM galib";
		$this->consulta($consulta);

		foreach ($this->marcas as $marca => $parametros){
			$consulta = "
				ALTER TABLE ".PREFIJO_SC.$this->client.".indmfr
				ADD COLUMN `".strtoupper($parametros["Nombre"])."` VARCHAR(13)";
			$this->consulta($consulta);
		}

		$this
			->mensaje("         Creando Made...")
			->eliminaTabla(PREFIJO_SC.$this->client.".made");

		$consulta = null;
		foreach ($this->sucursales as $branch => $parametros)
			$consulta .= "
				(SELECT $branch AS Branch,
					'{$parametros["Made"]}' AS Made,
					'{$parametros["DMS"]}' AS DMS)
				UNION ALL";

		foreach ($this->marcas as $marca => $parametros)
			$consulta .= "
				(SELECT $marca AS Branch,
					'{$parametros["Nombre"]}' AS Made,
					NULL AS DMS)
				UNION ALL";

		$this->eliminaTabla("made");

		$consulta = "
			CREATE TEMPORARY TABLE made AS
			SELECT *
			FROM (
				".substr($consulta, 0, -10)."
			) AS t";
		$this->consulta($consulta);

		$this->clonaTabla("made", PREFIJO_SC.$this->client.".made");

		$this
			->mensaje("         Creando Division...")
			->eliminaTabla(PREFIJO_SC.$this->client.".division");

		$consulta = null;
		foreach ($this->sucursales as $branch => $parametros)
			$consulta .= "
				(SELECT $branch AS Branch,
					'{$parametros["Division"]}' AS Division)
				UNION ALL";

		if (count($this->divisiones))
			foreach ($this->divisiones as $division => $parametros)
				$consulta .= "
					(SELECT $division AS Branch,
						'{$parametros["Nombre"]}' AS Division)
					UNION ALL";

		$this->eliminaTabla("division");

		$consulta = "
			CREATE TEMPORARY TABLE division AS
			SELECT *
			FROM (
				".substr($consulta, 0, -10)."
			) AS t";
		$this->consulta($consulta);

		$consulta = "DELETE FROM division WHERE Division = ''";
		$this->consulta($consulta);

		$this->clonaTabla("division", PREFIJO_SC.$this->client.".division");

		$this
			->mensaje("         Creando Industry...")
			->eliminaTabla(PREFIJO_SC.$this->client.".industry");

		$consulta = "
			CREATE TABLE ".PREFIJO_SC.$this->client.".industry AS
			SELECT {$this->industria} AS Industry";
		$this->consulta($consulta);

		$this->mensaje();

		return $this;
	}

	protected function exportaDetalles() {
		$this->depurar(__METHOD__);

		$this
			->usarBaseDatos(PREFIJO_SC.$this->client, true)
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("      Exportando {$this->meses_detalles} meses de detalles...");

		$tablas = array();

		$consulta = "
			SELECT t.Name, t.Tipo,
				if(t.Tipo = 'B', 'Date', g.Campofecha) AS Campofecha
			FROM tablas AS t
			LEFT OUTER JOIN galib AS g
				ON t.Name = g.Reporte
					AND ifnull(g.Estimatecalcvalue, 0) = 0
					AND g.Campofecha NOT IN ('Date', 'FechaCompra')
			WHERE ifnull(t.Planta, '') = ''
			GROUP BY t.Name";
		if ($resultado = $this->consulta($consulta)) {
			while ($fila = $resultado->fetch_assoc())
				$tablas[strtolower($fila["Name"])] = $fila;
			$resultado->close();

			foreach ($tablas as $tabla => $parametros) {
				if ($this->existeTabla($tabla))
					if ($campo_fecha = $parametros["Campofecha"]) {
						$this->mensaje("         '$tabla'...");

						$filtro_fecha = null;
						if ($parametros["Tipo"] != "B") {
                            $mesesDetalles = $this->meses_detalles - 1;
							$filtro_fecha = "AND $campo_fecha BETWEEN ".date("Ymd", strtotime("-{$mesesDetalles} month", strtotime(date("Ym01", $this->ayer))))." AND ".date("Ymd", $this->ayer);
						}

						$db_tbl = PREFIJO_SC.$this->client.".$tabla";
						$this->eliminaTabla($db_tbl);

						$consulta = "
							CREATE TABLE $db_tbl AS
							SELECT *
							FROM $tabla
							WHERE Branch IN (".implode(", ", array_keys($this->sucursales)).")
								$filtro_fecha";
						$this->consulta($consulta);

						$id_registro = null;
						if ($this->existeCampo("IdRegistro", $db_tbl))
							$id_registro = "DROP IdRegistro,";

						$consulta = "
							ALTER TABLE $db_tbl
							$id_registro
							DROP Client";
						$this->consulta($consulta);

						if (!$this->existeCampo("Name", $db_tbl)) {
							$consulta = "ALTER TABLE $db_tbl ADD COLUMN `Name` varchar(30) first";
							$this->consulta($consulta);
						}

						$consulta = "UPDATE crm_simetrical.clients AS clients, $db_tbl AS reporte
									SET reporte.Name = clients.Name
									WHERE clients.Client = {$this->client}
									AND reporte.Branch = clients.Branch";
						$this->consulta($consulta);
					}
			}
		}

		$this->mensaje();

		return $this;
	}

	protected function exportaMac()
	{
		$this->depurar(__METHOD__);

		$this
			->usarBaseDatos(PREFIJO_SIM.$this->client, true);

		if ($this->existeTabla("mac") && !$this->esTablaVacia("mac") && !$this->esTablaVacia("chart")) {
			$this
				->usarBaseDatos(PREFIJO_SC.$this->client, true);

			$db = PREFIJO_SIM.$this->client;
			$campos_indice = array("Client", "Branch", "Account", "SubAcc1", "SubAcc2", "SubAcc3", "SubAcc4", "CostCenter", "Type");
			$campos_chart = "`".implode("`,`", $campos_indice)."`";

			$this->mensaje("          Creando temporal de chart...");

			$this->consulta("DROP TABLE IF EXISTS `balanz`");
			$this->consulta("DROP TABLE IF EXISTS `gamac`");

			$this->consulta("DROP TABLE IF EXISTS `tmp_chart`");
			$this->consulta("DROP TABLE IF EXISTS `tmp_mac`");

			$this->consulta("CREATE TABLE tmp_chart LIKE {$db}.chart");

			$this->consulta("CREATE UNIQUE INDEX indtmpchart ON tmp_chart ($campos_chart)");

			$this->consulta("INSERT INTO tmp_chart SELECT * FROM {$db}.chart GROUP BY $campos_chart");


			$this->mensaje("          Creando temporal de mac...");
			array_push($campos_indice, "Date");
			$campos_mac = "`".implode("`,`", $campos_indice)."`";
			$_2meses = date("Y-m-d", strtotime("-2 month", strtotime(date("Y-m-01", $this->ayer))));

			$this->consulta("CREATE TABLE tmp_mac LIKE {$db}.mac");

			$consulta = "ALTER TABLE tmp_mac ";
			$change   = array();
			foreach (range(1,7) as $i) {
				$change[] = "CHANGE COLUMN Ref{$i} Ref{$i} VARCHAR(57)";
			}
			$consulta .= implode(",", $change);
			$this->consulta($consulta);

			$this->consulta("CREATE UNIQUE INDEX indtmpmac ON tmp_mac ($campos_mac)");

			$this->consulta("INSERT INTO tmp_mac SELECT * FROM {$db}.mac WHERE `Date` >= '{$_2meses}' GROUP BY $campos_mac");

            // Actualizando campos de new_mac y new_char
            $camposUpdate = array(
                'Account',
                'SubAcc1',
                'SubAcc2',
                'SubAcc3',
                'SubAcc4',
                'CostCenter',
            );
            $tablasUpdate = array('tmp_mac', 'tmp_chart');

            foreach ($tablasUpdate as $tablaUp) {
                foreach ($camposUpdate as $campoUp) {
                    $consultaUp = '
                        update ignore `' . $tablaUp . '` set ' . $campoUp . ' = "" where ' . $campoUp . ' is null
                    ';
                    $this->consulta($consultaUp);
                }
            }

			$this->mensaje("          Actualizando referencias...");

			$change   = array();
			$changeTo = array();
			$consulta = "UPDATE tmp_mac SET ";
			foreach (range(1,7) as $i) {
				$change[] = "Ref{$i} = NULL";
				$changeTo[] = "m.Ref{$i} = c.Ref{$i}";
			}
			$consulta .= implode(",", $change);
			$this->consulta($consulta);

			$consulta = "UPDATE tmp_mac AS m,
				tmp_chart AS c
				SET m.Type = c.Type, ";

			$consulta .= implode(",", $changeTo)."
				WHERE m.Client = c.Client
				AND m.Branch = c.Branch
				AND m.Account = c.Account
				AND m.SubAcc1 = c.SubAcc1
				AND m.SubAcc2 = c.SubAcc2
				AND m.SubAcc3 = c.SubAcc3
				AND m.SubAcc4 = c.SubAcc4
                AND m.CostCenter = c.CostCenter
                ";
			$this->consulta($consulta);

			$this->mensaje("          Actualiza MAC con referencias de CHART...");

			$consulta = "UPDATE {$db}.mac AS m,
			tmp_chart AS c
			SET m.Type = c.Type, ";

			$consulta .= implode(",", $changeTo)."
				WHERE m.Client = c.Client
				AND m.Branch = c.Branch
				AND m.Account = c.Account
				AND m.SubAcc1 = c.SubAcc1
				AND m.SubAcc2 = c.SubAcc2
				AND m.SubAcc3 = c.SubAcc3
				AND m.SubAcc4 = c.SubAcc4
				AND m.CostCenter = c.CostCenter
				";
			$this->consulta($consulta);

			$this->consulta("DELETE FROM tmp_mac WHERE IFNULL(Type, '') = ''");

			foreach (range(1,7) as $i) {
				$this->mensaje("              Ref{$i} - GALIB/MAC...");
				$consulta = "UPDATE tmp_mac AS m,
						galib AS g
					SET Ref{$i} = CONCAT_WS('-', g.Name, g.Id)
					where m.Ref{$i} = g.Id";
				$this->consulta($consulta);
			}

			if (!$this->existeCampo("Name", 'tmp_mac')) {
				$consulta = "ALTER TABLE tmp_mac ADD COLUMN `Name` VARCHAR(30) FIRST";
				$this->consulta($consulta);
			}
			$consulta = "UPDATE crm_simetrical.clients AS clients, tmp_mac AS reporte
					SET reporte.Name = clients.Name
					WHERE clients.Client = {$this->client}
					AND reporte.Branch = clients.Branch";
			$this->consulta($consulta);

			$this->mensaje("          Creando el reporte GAMAC...");

			$iMes2 = date("Y-m-d", strtotime("-1 month", strtotime(date("Y-m-01", $this->ayer))));
			$fMes2 = date("Y-m-d", strtotime("-1 day", strtotime(date("Y-m-01", $this->ayer))));

			$this->consulta("CREATE TABLE gamac LIKE tmp_mac");
			$this->consulta("INSERT INTO gamac SELECT * FROM tmp_mac WHERE Date BETWEEN '{$iMes2}' AND '{$fMes2}'");

			$consulta = "DELETE FROM tmp_mac ";
			$where    = array();
			foreach (range(1,7) as $i) {
				$where[] = "IFNULL(Ref{$i}, '') = ''";
			}
			$consulta .= ' WHERE '.implode(' AND ', $where);
			$this->consulta($consulta);

			$this->mensaje("          Creando el reporte BALANZ...");

			$this->consulta("RENAME TABLE tmp_mac TO balanz");

			$this->consulta("DROP TABLE IF EXISTS `tmp_chart`");
			$this->consulta("DROP TABLE IF EXISTS `tmp_mac`");

			if ($this->existeTabla(PREFIJO_SC.$this->client.".balanz")) {
				$consulta = "INSERT INTO ".PREFIJO_SC.$this->client.".gafiles (Detalles, Reporte, Dept) VALUES ('Balanza','BALANZ',6)";
				$this->consulta($consulta);
				$this->mensaje("             Agregando Balanz...");
			} else {
				$this->mensaje("             No existe Balanz...");
			}

		}

		return $this;
	}

	protected function creaTablaAsesoresVendedores() {
		$this->depurar(__METHOD__);

		$this
			->mensaje("      Creando Tablas de Asesores y Vendedores Activos")
			->mensaje("         Cliente ".$this->client);

		$basedatos =  $this->baseDatosActual();
		$this->usarBaseDatos(PREFIJO_SIM.$this->client, true);

		$this->eliminaTabla("Galibtm2");

		if ($this->existeTabla("tablas")) {
			$tablas = "tablas";
		} elseif ($this->existeTabla("indice.auto-tbl")){
			$tablas = "indice.`auto-tbl`";
		}
			$consulta = "
			CREATE TABLE Galibtm2
				(SELECT
				    g.Id,
				    LOWER(g.Reporte) as Reporte,
				    g.CampoClave,
				    g.Campofiltro1,
				    g.Filtro1,
				    g.Campofiltro2,
				    g.Filtro2,
				    g.Campofiltro3,
				    g.Filtro3,
				    g.Campofiltro4,
				    g.Filtro4,
				    g.Campofiltro5,
				    g.Filtro5,
				    g.Operacion,
				    g.CampoType,
				    g.Type AS `Type`
				FROM
				    galib AS g
				        INNER JOIN
				    $tablas AS t ON g.`Reporte` = t.`NAME` AND g.kind = 'B'
				        AND g.Id like 'I%'
						AND g.campoFecha = 'Date'
				        AND g.operacion = 'COUNT'
				        AND g.EstimateCalcValue IS NULL
				        AND t.tipo <> 'B')";
			$this->consulta($consulta);

		if ($this->existeTabla("Galibtm2")) {

			$resultado = $this->obtenerTodo("Galibtm2");
			$this->depurar(print_r($resultado,true));

			foreach ($resultado as $key => $value) {
				$this->id = $value["Id"];
				$this->Reporte = $value["Reporte"];
				$this->CampoClave = $value["CampoClave"];
				$this->Operacion = $value["Operacion"];
				$this->CampoType = $value["CampoType"];
				$this->Type = $value["Type"];

				$this->eliminaTabla("db".strtolower($this->id));

				if ($this->existeTabla($this->Reporte)) {

					if ($this->existeCampo("{$this->CampoClave}","{$this->Reporte}")) {
						$consulta = "
						CREATE TABLE `db".strtolower($this->id)."` (
						SELECT
							`Client`,
							`Branch`,
							`{$this->CampoClave}`";

							if ($this->existeCampo("VentasNetas", $this->Reporte)){
								$consulta .= ",SUM(VentasNetas) as `Ventas30d`";
								$orden = " ORDER BY `Ventas30d`";
							} elseif ($this->existeCampo("VentasNetas_", $this->Reporte)) {
								$consulta .= ",SUM(VentasNetas_) as `Ventas30d`";
								$orden = " ORDER BY `Ventas30d`";
							} elseif (!$this->existeCampo("VentasNetas_", $this->Reporte) && !$this->existeCampo("VentasNetas",$this->Reporte)) {
								if ($this->existeCampo("NumeroOT", $this->Reporte)) {
									$consulta .= ",COUNT(distinct NumeroOT) as `Ordenes30d`";
									$orden = " ORDER BY `Ordenes30d` DESC";
								}
							}

						$consulta .= " ,'".date('Y-m-d', $this->hoy)."' as `Date`";
						$consulta .= " FROM $this->Reporte ";
						$consulta .= " WHERE ";

						$filtros = null;
						$continuar = true;
						for ($i = 1; $i <= 5; $i++) {
							if ($campo_filtro = $value["Campofiltro$i"]) {
								if ($continuar = $this->existeCampo($campo_filtro, $this->Reporte) && $continuar)
									if ($continuar = ((!is_null($value["Filtro$i"]) && $value["Filtro$i"] != "") && $continuar))
										$filtros .= $this->traduceFiltro($value["Filtro$i"].", not blank", $campo_filtro, $this->Reporte, null, 0)." AND ";
							}
						}

						if (isset($filtros)) {
							$consulta .= substr($filtros, 0, -4);
						}

						$consulta .= " GROUP BY {$this->CampoClave}, Branch )";

						if($this->consulta($consulta)){
							$this->mensaje("            'db".strtolower($this->id)."'");
						}
					} else {
						$this->mensaje("               No existe el Campo Clave para 'db".strtolower($this->id)."'");
					}
				}
			}
		}

		$this->usarBaseDatos($basedatos);
		return $this;
	}

	protected function aplicaTypes() {
		$this->depurar(__METHOD__);

		$this
			->usarBaseDatos(PREFIJO_SC.$this->client, true)
			->mensaje("      Aplicando los Types...");

		$types = array();

		$consulta = "
			SELECT DISTINCT Reporte, Campo
			FROM types";
		if ($resultado = $this->consulta($consulta)) {
			while($fila = $resultado->fetch_assoc())
				$types[strtolower($fila["Reporte"])][] = $fila["Campo"];
			$resultado->close();

			foreach ($types as $reporte => $campos) {
				if ($this->existeTabla($reporte)) {
					$this->mensaje("         '$reporte'...");

					foreach ($campos as $campo) {
						$this->mensaje("            Agregando 'T".substr($campo, 4)."'...");
						$addIndex = '';
						$addCampo = '';

						if (!$this->existeCampo('T' . substr($campo, 4), $reporte)) {
							$addCampo = ' ADD T' . substr($campo, 4) . ' VARCHAR(10)';
						}

						if (!$this->existeIndice($campo . '_Branch', $reporte)) {
							$addIndex = ' ADD INDEX ' . $campo . '_Branch (' . $campo . ', Branch)';
						}

						if (!empty($addIndex) || !empty($addCampo)) {
							$consulta = 'ALTER TABLE ' . $reporte;

							if (!empty($addCampo)) {
								$consulta .= $addCampo . ',';
							}

							if (!empty($addIndex)) {
								$consulta .= $addIndex . ',';
							}

							$this->consulta(trim($consulta, ', '));
						}

						$this->mensaje("               Asignando Types...");

						$consulta = "
							UPDATE $reporte AS r, types AS t
							SET r.T".substr($campo, 4)." = t.Type
							WHERE t.Reporte = '$reporte'
								AND t.Campo = '$campo'
								AND r.Branch = t.Branch
								AND r.$campo = t.Tipo";
						$this->consulta($consulta);
					}
				}
			}
		}

		$this->mensaje();

		return $this;
	}

	protected function generaPlanta() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102RPLT', 'Genera Reportes Planta']);
		$this->identificaCliente();

		if ($this->client) {
			$this
				->usarBaseDatos(PREFIJO_SC.$this->client, true)
				->mensaje("      Genera reportes de planta...");

			if (count($this->marcas)) {
				$consulta = "
					SELECT lower(Name) as Tabla
					FROM tablas
					WHERE ifnull(Planta, '') <> ''";
				if ($resultado = $this->consulta($consulta)) {
					while ($fila = $resultado->fetch_assoc()) {
						$this->eliminaTabla($fila["Tabla"]);
						// $this->eliminaTabla(PREFIJO_SIM.$this->client.".".$fila["Tabla"]);
					}

					$resultado->close();
				}

				if ($dsn = $this->dsn)
					$dsn = "dsn=$dsn";

				$php = "php";
				if (SO == "WINDOWS")
					$php = "%php53%";

				$depurar = null;
				if ($this->depurar)
					$depurar = "depurar";

				$ignorar = null;
				if ($this->ignorar)
					$ignorar = "ignorar";

				foreach ($this->marcas as $marca => $parametros) {
					$this->mensaje("         Generando '{$parametros["Nombre"]}'...");
					$archivo = dirname(__FILE__)."/".str_replace(" ", "_", strtolower($parametros["Nombre"])).".php";
					if ($this->existeArchivo($archivo, null)) {
						$this->guardaLog(['01x0102REPL', "Planta {$parametros["Nombre"]}"]);
						$hoy = date('Y-m-d', $this->hoy);
						$this->mensaje();

							$comando = "$php -f $archivo -- $dsn client={$this->client} hoy={$hoy} $depurar $ignorar";
							if ($this->ejecutaComando($comando, false, true)){
								$this->depurar("Ejecutando $archivo ".$parametros["Nombre"]);
							} else {
								$this->error("No se concluyo el plan de trabajo de $marca -".$parametros["Nombre"], false);
								$this->guardaLog(['01x0101NPLA', 'No se concluyo el plan de trabajo Genera Planta ' . $marca . '-' . $parametros["Nombre"]]);
							}

						$this->guardaLog(['02x0102REPL', "Planta {$parametros["Nombre"]}"]);
						$this->mensaje();
					} else {
						$this->mensaje("            No existe plan de trabajo");
					}
				}

				if ($this->existeTabla("mfrobj")) {
					$this->eliminaTabla(PREFIJO_SIM.$this->client.".mfrobj");

					$consulta = "
						CREATE TABLE ".PREFIJO_SIM.$this->client.".mfrobj AS
						SELECT *
						FROM ".PREFIJO_SC.$this->client.".mfrobj";
				}
			}

			$this->mensaje();
		}
		$this->guardaLog(['02x0102RPLT', 'Genera Reportes Planta']);
		return $this;
	}

	protected function creaDetalles() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102CDET', 'Creacion de Tablas de detalles']);

		$this->identificaCliente();

		if ($this->client) {
			$this->mensaje("   Creando parametros y detalles...");

			$this
				->creaTablasParametro()
				->exportaDetalles()
				->creaTablaAsesoresVendedores()
				->exportaMac()
				->aplicaTypes();

			if ($this->plantas)
				$this->generaPlanta();
		}

		$this->guardaLog(['02x0102CDET', 'Creacion de Tablas de detalles']);

		return $this;
	}

	protected function obtenHistorico() {

        $this->depurar(__METHOD__);

        $this->mensaje("Obtenemos fecha historico a procesar de reportes operativos...");

        $consulta = "SELECT * FROM mfr.marca AS m
                    left join crm_simetrical.clients AS c
                    ON (m.Marca = c.Made)
                    WHERE c.Client = {$this->client}
                    AND m.History = '1'";
        if ($resultado = $this->consulta($consulta)) {
            while ($fila = $resultado->fetch_assoc()) {
                if ($fila['History'] > 0) {
                    $this->history = true;
                    $this->fechaInicioOperativo = $fila['FechaInicio'];
					$this->fechaFinOperativo = date('Y-m-d',$this->ayer);
                }
            }
        }

        $this->mensaje($this->fechaInicioOperativo);
		$this->mensaje($this->fechaFinOperativo);

        return $this;
    }

	protected function exportaOperativos($meses = 2) {
		$this->depurar(__METHOD__."($meses)");
		$this->guardaLog(['01x0102EOPE', 'Exportacion de Tablas Operativos']);

		$this->identificaCliente();

		$this
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("   Exportando reportes operativos al DBU...");

		if ($this->existeTabla('indice.reportes_operativos')) {
			$consulta = "
			SELECT LOWER(Name) AS Reporte, Tipo, CampoFecha
			FROM indice.reportes_operativos
			WHERE IFNULL(CampoFecha, '') <> '' AND IFNULL(Planta, 0) <> 1 AND Activo = 1 ";

			if ($resultado = $this->consulta($consulta)) {
				$mysqldump = "mysqldump";
				if (SO == "WINDOWS")
					$mysqldump = "%mysqldump%";

				$this->decodificaDSN($this->dsn,
					$servidor, $usuario, $contrasena, $basedatos, $puerto);

				$mesesOriginal = $meses;

				while ($fila = $resultado->fetch_assoc())
					if ($this->existeTabla($fila["Reporte"]))
						if ($this->existeCampo("Branch", $fila["Reporte"]) &&
							$this->existeCampo($fila["CampoFecha"], $fila["Reporte"])) {

							$this->mensaje("      '{$fila["Reporte"]}'...");

							foreach ($this->sucursales as $sucursal => $parametros) {
								$this->mensaje("         '$sucursal'...");

								$directorio_base = RAIZ_SMIG."/".$this->marcas[(int)$parametros["MadeId"]]["NombreCorto"]."/".strtoupper($fila["Reporte"]);

								$this->obtenHistorico();

								if ($this->history) {
									$date1 = date_create($this->fechaInicioOperativo);
									$date2 = date_create($this->fechaFinOperativo);

									$intervalOperativo = date_diff($date1, $date2);
									$mesesOriginal = $intervalOperativo->format('%y') * 13 + $intervalOperativo->format('%m');
								}

								if ($fila["Tipo"] == "B") {
									$meses = 1;
								} else {
									$meses = $mesesOriginal;
								}

								$inicio = strtotime(date("Y-m-01", $this->ayer));
								for ($m = 1; $m <= $meses; $m++) {
									$fin = strtotime("+1 month", $inicio) - 1;

									$directorio = $directorio_base."/".date("Y", $fin)."/".date("m", $fin);
									if (!$this->existeDirectorio($directorio))
										$this->creaDirectorio($directorio);

									$archivo = $directorio."/".$this->client.str_pad($sucursal, 2, "0", STR_PAD_LEFT).".sql.dump";

									$this->mensaje("            '".date("Y", $fin)."-".date("m", $fin)."'...");

									$comando = "$mysqldump ".PREFIJO_SIM.$this->client." -h $servidor -P $puerto -u $usuario -p$contrasena --tables {$fila["Reporte"]} --compress --compatible=ansi --where=\"Branch = $sucursal AND {$fila["CampoFecha"]} BETWEEN ".date("Ymd", $inicio)." AND ".date("Ymd", $fin)."\" > $archivo";
									$this->ejecutaComando($comando, true, true);
									//$this->mensaje($comando);

									$inicio = strtotime("-1 month", $inicio);
								}
							}

							$this->mensaje();
						}
				$resultado->close();
			}

		} else {
			$this->mensaje("      No se encontro la tabla 'indice.reportes_operativos' necesario para la extraccion...\n");
		}

		$this->guardaLog(['02x0102EOPE', 'Exportacion de Tablas Operativos']);

		return $this;
	}

	protected function creaTablasCalculo() {
		$this->depurar(__METHOD__);

		$this->guardaLog(['01x0102CTCA', 'Creacion de Tablas de calculos de meses']);

		$this->identificaCliente();

		$this
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("   Crea tablas temporales para calculo...");

		$this
			->mensaje("      '14meses'...")
			->eliminaTabla("14meses");

		$consulta = null;
		$ano = date("Y", $this->ayer);
		$mes = date("m", $this->ayer);
		$dia = date("d", $this->ayer);
		for ($i = 1; $i <= 14; $i++) {
			$consulta .= "
				(SELECT $ano AS Ano, $mes AS Mes, $dia AS Dia)
				UNION ALL";

			$this->ano_final = $ano;
			$this->mes_final = $mes;

			$dia = date("d", strtotime(date("$ano-$mes-01")) - 1);
			if (($mes -= 1) == 0) {
				$ano -= 1;
				$mes = 12;
			}
		}

		$consulta = "
			CREATE TABLE 14meses AS
			SELECT *
			FROM (".substr($consulta, 0, -10)."
			) AS f
			ORDER BY Ano, Mes, Dia";
		$this->consulta($consulta);

		$consulta = "
			ALTER TABLE 14meses
			ADD INDEX AnoMesDia (Ano, Mes, Dia)";
		$this->consulta($consulta);

		if ($this->meses_proceso == 4) {
			$this
				->mensaje("      '4meses'...")
				->eliminaTabla("4meses");

			$consulta = "
				CREATE TABLE 4meses AS
				SELECT *
				FROM 14meses
				LIMIT 9, 5"; /* LIMIT 10, 4"; */
			$this->consulta($consulta);
		}

		$this
			->mensaje("      'amd'...")
			->eliminaTabla("amd");

		$consulta = "CREATE TABLE amd LIKE 14meses";
		$this->consulta($consulta);

		$consulta = "
			SELECT *
			/*FROM {$this->meses_proceso}meses*/
			FROM 14meses";
		if ($resultado = $this->consulta($consulta)) {
			$consulta = null;
			while ($fila = $resultado->fetch_assoc()) {
				for ($i = 1; $i <= $fila["Dia"]; $i++)
					$consulta .= "
						(SELECT {$fila["Ano"]}, {$fila["Mes"]}, $i)
						UNION ALL";
			}
			$resultado->close();

			$consulta = "
				INSERT INTO amd
				".substr($consulta, 0, -10);
			$this->consulta($consulta);
		}

		$this
			->mensaje("      '2y2meses'...")
			->eliminaTabla("2y2meses");

		$consulta = "
			CREATE TABLE 2y2meses AS
			SELECT *
			FROM 14meses
			LIMIT 2";
		$this->consulta($consulta);

		$consulta = "
			INSERT INTO 2y2meses
			SELECT *
			FROM 14meses
			LIMIT 11, 3";
		$this->consulta($consulta);

		$this->mensaje("      'containr'...");
		$this->eliminaTabla("containr");

		$consulta = "
			DELETE FROM gestion
			WHERE Ano = ".date("Y", $this->ayer)."
				AND Mes = ".date("m", $this->ayer)."
				AND Dia >= ".date("d", $this->ayer);
		$this->consulta($consulta);

		$consulta = "
			CREATE TABLE containr (
				Branch SMALLINT UNSIGNED,
				Ind CHAR(4),
				Ano SMALLINT UNSIGNED,
				Mes SMALLINT UNSIGNED,
				Dia SMALLINT UNSIGNED,
				UNIQUE INDEX _Principal (Branch, Ind, Ano, Mes, Dia)
			)";
		$this->consulta($consulta);

		$this->eliminaTabla("temp");

		// Se agrega una eliminación extra ya que el anterior sí elimina pero queda una tabla temp no temporal
		$this->eliminaTabla("temp");

		$consulta = "CREATE TABLE temp LIKE containr";
		$this->consulta($consulta);

		$consulta = "
			INSERT INTO temp
			SELECT gt.Branch, gt.Ind, gt.Ano, gt.Mes, gt.Dia
			FROM gestion AS gt
			INNER JOIN galib AS gl
				ON gt.Ind = gl.Id
			WHERE gt.Branch IN (".implode(", ", array_keys($this->sucursales)).")
				AND gl.Id LIKE 'I%'
				AND ifnull(gl.Estimatecalcvalue, 0) = 0
			GROUP BY gt.Branch, gt.Ind, gt.Ano, gt.Mes, gt.Dia";
		$this->consulta($consulta);

		$sucursales = null;
		foreach ($this->sucursales as $branch => $parametros)
			$sucursales .= "
				(SELECT $branch AS Branch)
				UNION ALL";

		$consulta = "
			INSERT INTO containr
			SELECT b.Branch, g.Id AS Ind, f.Ano, f.Mes, f.Dia
			FROM galib AS g
			CROSS JOIN amd AS f
			CROSS JOIN (
				".substr($sucursales, 0, -10)."
			) AS b
			WHERE g.Id LIKE 'I%'
				AND ifnull(g.Estimatecalcvalue, 0) = 0
				AND NOT EXISTS (
					SELECT *
					FROM temp
					WHERE Branch = b.Branch
						AND Ind = g.Id
						AND Ano = f.Ano AND Mes = f.Mes AND Dia = f.Dia
				)";
		if ($this->consulta($consulta))
			$this->mensaje("         ".$this->filas_afectadas." registros insertados");

		$this
			->mensaje("      'ceros'...")
			->eliminaTabla("ceros");

		$consulta = "
			CREATE TABLE ceros (
				Branch SMALLINT UNSIGNED,
				Ind CHAR(4),
				Ano SMALLINT UNSIGNED,
				Mes SMALLINT UNSIGNED,
				Dia SMALLINT UNSIGNED,
				UNIQUE INDEX _Principal (Branch, Ind, Ano, Mes, Dia)
			)";
		$this->consulta($consulta);

		$this->mensaje();

		$this->guardaLog(['02x0102CTCA', 'Creacion de Tablas de calculos de meses']);

		return $this;
	}

	protected function preparaGestion() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102PGES', 'Preparacion de la tabla gestion']);

		$this
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("   Preparando Gestion y tablas relacionadas...");

		$this->mensaje("      Eliminando registros con mas de 14 meses...");

		$consulta = "
			DELETE FROM gestion
			WHERE Ano < {$this->ano_final}
				OR (Ano = {$this->ano_final} AND Mes < {$this->mes_final})";
		if ($this->consulta($consulta))
			$this->mensaje("         ".$this->filas_afectadas." registros eliminados");

		$this->mensaje("      Eliminando registros de sucursales virtuales...");

		$this->eliminaTabla("virtuals");

		$this->preparavirtuals();

		$this->mensaje("      Insertando registros nuevos en cero...");

		$consulta = "
			INSERT INTO gestion
				(Client, Branch, Ano, Mes, Dia, Ind, Valor)
			SELECT {$this->client}, Branch, Ano, Mes, Dia, Ind, 0
			FROM containr";
		if ($this->consulta($consulta))
			$this->mensaje("         ".$this->filas_afectadas." registros insertados");

		$this->mensaje("      Eliminando registros en el futuro...");

		$reportes = array();

		$consulta = "
			SELECT DISTINCT LOWER(Reporte) AS Reporte, Campofecha
			FROM galib
			WHERE ifnull(Campofecha, '') <> 'Date'
				AND ifnull(Estimatecalcvalue, 0) = 0";
		if ($resultado = $this->consulta($consulta)) {
			while ($fila = $resultado->fetch_assoc())
				$reportes[$fila["Reporte"]][] = $fila["Campofecha"];
			$resultado->close();

			foreach ($reportes as $reporte => $campos) {
				if ($this->existeTabla($reporte)) {
					$this->mensaje("         '$reporte'...");

					$consulta = null;
					foreach ($campos as $campo){
						if ($this->existeCampo($campo,$reporte)) {
							$consulta .= "$campo > ".date("Ymd", $this->ayer)." OR ";
						}else{
							$this->mensaje("            No existe el campo $campo en el reporte $reporte");
							$this->guardaLog(['01x0101INDI', 'No existe el campo ' . $campo . ' en el reporte ' . $reporte]);

						}
					}
					
					if (!is_null($consulta)) {
						$consulta = "
							DELETE FROM $reporte
							WHERE ".substr($consulta, 0, -4);
						if ($this->consulta($consulta)){
							$this->mensaje("            ".$this->filas_afectadas." registros eliminados");
						}
					}
				}
			}
		}

		$this->mensaje();

		$this->guardaLog(['02x0102PGES', 'Preparacion de la tabla gestion']);

		return $this;
	}

	protected function indicadoresBalance() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102INDB', 'Asignacion de valores de indicadores de balance']);

		$this
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("   Asignando el Valor al Saldo faltante...");

		$consulta = "
			UPDATE gestion AS gt, galib AS gl
			SET gt.Saldo = gt.Valor
			WHERE gt.Ind = gl.Id
				AND gl.Id LIKE 'I%'
				AND gl.Kind = 'B'
				AND ifnull(gl.Shared, '') = ''
				AND ifnull(gt.Valor, 0) <> 0
				AND ifnull(gt.Saldo, 0) = 0";
		if ($this->consulta($consulta))
			$this->mensaje("      ".$this->filas_afectadas." registros actualizados");

		$this->mensaje("   Arrastra saldos de Balance Ascendente...");

		$this->eliminaTabla("temp");

		$consulta = "
			CREATE TEMPORARY TABLE temp AS
			SELECT gt.Client, gt.Branch, gt.Ind, gt.Ano, gt.Mes,
				MAX(Valor) AS ValorMAX, MIN(Valor) AS ValorMIN
			FROM gestion AS gt
			INNER JOIN galib AS gl
				ON gt.Ind = gl.Id
			WHERE gl.Id LIKE 'I%' AND gl.Kind = 'B'
			GROUP BY gt.Branch, gt.Ind, gt.Ano, gt.Mes";
		$this->consulta($consulta);

		$this->eliminaTabla("temp2");

		$consulta = "
			CREATE TEMPORARY TABLE temp2
			SELECT Branch, Ind, Ano, Mes
			FROM temp
			WHERE ValorMAX > 0
				AND ifnull(ValorMIN, 0) = 0";
		$this->consulta($consulta);

		$consulta = "
			ALTER TABLE temp2
			ADD UNIQUE INDEX _Principal (Branch, Ind, Ano, Mes)";
		$this->consulta($consulta);

		$this->eliminaTabla("tmpbal");

		$consulta = "
			CREATE TEMPORARY TABLE tmpbal AS
			SELECT Client, Branch, Ind, Ano, Mes, Dia, Valor, Saldo
			FROM gestion AS gt
			INNER JOIN temp2 AS t
				USING (Branch, Ind, Ano, Mes)";
		$this->consulta($consulta);

		$consulta = "
			ALTER TABLE tmpbal
			ADD UNIQUE INDEX _Principal (Client, Branch, Ind, Ano, Mes, Dia)";
		$this->consulta($consulta);

		$this->eliminaTabla("tmpdia1");

		$consulta = "
			CREATE TEMPORARY TABLE tmpdia1 AS
			SELECT *
			FROM tmpbal
			WHERE Dia = 1
				AND ifnull(Valor, 0) = 0";
		$this->consulta($consulta);

		$this->eliminaTabla("tmp14mes");

		$consulta = "
			CREATE TEMPORARY TABLE tmp14mes AS
			SELECT *
			FROM 14meses";
		$this->consulta($consulta);

		$consulta = "
			ALTER TABLE tmp14mes
			ADD Cardinal INT NOT NULL KEY AUTO_INCREMENT,
			ADD INDEX AnoMes (Ano, Mes)";
		$this->consulta($consulta);

		$this->eliminaTabla("temp");

		$consulta = "
			CREATE TEMPORARY TABLE temp AS
			SELECT d.*, m.Cardinal
			FROM tmpdia1 AS d
			INNER JOIN tmp14mes AS m
				USING (Ano, Mes)";
		$this->consulta($consulta);

		$consulta = "
			UPDATE temp
			SET Cardinal = Cardinal - 1";
		$this->consulta($consulta);

		$consulta = "
			REPLACE INTO tmpbal
			SELECT t.Client, t.Branch, t.Ind, t.Ano, t.Mes, t.Dia,
				g.Valor, g.Saldo
			FROM temp AS t
			INNER JOIN tmp14mes AS m
				USING (Cardinal)
			INNER JOIN gestion AS g
				ON t.Ind = g.Ind
					AND m.Ano = g.Ano AND m.Mes = g.Mes AND m.Dia = g.Dia";
		$this->consulta($consulta);

		$consulta = "
			SELECT *
			FROM tmpbal
			ORDER BY Client, Branch, Ind, Ano, Mes, Dia";
		if ($resultado = $this->consulta($consulta)) {
			$ind = null;
			$valor = null;
			while ($fila = $resultado->fetch_assoc()) {
				if ($fila["Ind"] == $ind) {
					if ($fila["Valor"] == 0) {
						$consulta = "
							UPDATE tmpbal
							SET Valor = $valor
							WHERE Client = {$fila["Client"]}
								AND Branch = {$fila["Branch"]}
								AND Ind = '{$fila["Ind"]}'
								AND Ano = {$fila["Ano"]}
								AND Mes = {$fila["Mes"]}
								AND Dia = {$fila["Dia"]}";
						$this->consulta($consulta);
					} else {
						$valor = ($fila["Valor"] == null) ? 0 : $fila["Valor"];
					}
				} else {
					$ind = $fila["Ind"];
					$valor = ($fila["Valor"] == null) ? 0 : $fila["Valor"];
				}
			}
			$resultado->close();

			$consulta = "
				UPDATE tmpbal
				SET Saldo = Valor";
			$this->consulta($consulta);

			$consulta = "
				REPLACE INTO gestion
					(Client, Branch, Ind, Ano, Mes, Dia, Valor, Saldo)
				SELECT *
				FROM tmpbal
				-- ON DUPLICATE KEY UPDATE Valor = VALUES(Valor), Saldo = VALUES(Saldo)";
			if ($this->consulta($consulta))
				$this->mensaje("      ".$this->filas_afectadas." registros actualizados");
		}

		$this->mensaje();

		$this->guardaLog(['02x0102INDB', 'Asignacion de valores de indicadores de balance']);

		return $this;
	}

	protected function generaIndicadores() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102GIND', 'Creacion de indicadores']);

		$this->identificaCliente();

		$this
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("   Generando indicadores...");

		$ayer = date("Ymd", $this->ayer);
		$inicio = date("Ymd", strtotime("-{$this->meses_proceso} month", strtotime(date("Ym01", $this->ayer))));

		$sucursales = null;
		foreach ($this->sucursales as $branch => $parametros)
			$sucursales .= "
				(SELECT $branch AS Branch)
				UNION ALL";

		$this->eliminaTabla("paso");

		$consulta = "
			CREATE TEMPORARY TABLE paso (
				Client SMALLINT UNSIGNED,
				Branch SMALLINT UNSIGNED,
				Ind CHAR(4),
				Kind CHAR(1),
				FechaInd DATE,
				Ano SMALLINT UNSIGNED,
				Mes SMALLINT UNSIGNED,
				Dia SMALLINT UNSIGNED,
				Semana SMALLINT UNSIGNED,
				Valor DECIMAL(18,4)
			)";
		$this->consulta($consulta);

		if ($this->existeTabla('contables')) {
			$campos = $this->listaCampos('contables');
			$consulta = "INSERT INTO paso ($campos) SELECT $campos FROM contables";
			$this->consulta($consulta);

			$this->eliminaTabla("contables");
		}

		$consulta = "
			SELECT *
			FROM galib
			WHERE ifnull(Reporte, '') <> ''
				AND ifnull(Estimatecalcvalue, 0) = 0
			ORDER BY Dept, Reporte, Id";
		if ($resultado = $this->consulta($consulta)) {
			while ($fila = $resultado->fetch_assoc()) {
				$reporte = strtolower($fila["Reporte"]);
				if ($this->existeTabla($reporte)) {
					$indicador = $fila["Id"];
					$campo_clave = $fila["Campoclave"];
					$operacion = str_replace("AVERAGE", "AVG", trim(strtoupper($fila["Operacion"])));

					$distinto = null;
					if ($operacion == "COUNT")
						$distinto = "DISTINCT";

					$campo_type = $fila["Campotype"];

					$filtros = null;
					$continuar = true;
					for ($i = 1; $i <= 5; $i++)
						if ($campo_filtro = $fila["Campofiltro$i"]) {
							if ($continuar = $this->existeCampo($campo_filtro, $reporte) && $continuar)
							if ($continuar = ((!is_null($fila["Filtro$i"]) && $fila["Filtro$i"] != "") && $continuar))
								$filtros .= $this->traduceFiltro($fila["Filtro$i"], $campo_filtro, $reporte)." AND ";
						}

					$continuar = (in_array($operacion, array("SUM", "COUNT", "AVG"))) && $continuar;

					$continuar = ($this->existeCampo($campo_clave, $reporte)) && $continuar;

					$types = null;
					if ($campo_type && strlen($fila["Type"]) > 0) {
						$continuar = ($this->existeCampo($campo_type, $reporte)) && $continuar;

						$filtro = $this->traduceFiltro($fila["Type"], "Type", "types", "t.");

						$types = "
							INNER JOIN types AS t
								ON t.Branch = r.Branch AND t.Reporte = '$reporte'
									AND t.Campo = '$campo_type' AND (t.Tipo = r.$campo_type OR (ifnull(t.Tipo,'') = ifnull(r.$campo_type,'')))
									AND $filtro";
					}

					if ($continuar) {
						$this->mensaje("      $indicador - {$fila["Name"]}...");

						if (($campo_fecha = $fila["Campofecha"]) == "Date") {
							if ($this->existeCampo($campo_fecha, $reporte)) {
								if ($fila["Kind"] == "B") {
									$campo_fecha = "r.`Date`";
								} else {
									$campo_fecha = $ayer;
								}
							} else {
								$campo_fecha = $ayer;
							}
						}

						if ($this->existeCampo('Client', $reporte)) {
							$consulta = "
							INSERT INTO paso
								(Client, Branch, Ind, Kind, FechaInd, Valor)
							SELECT {$this->client}, b.Branch,
								'{$indicador}', '{$fila["Kind"]}',
								i.FechaInd, i.Valor
							FROM (
								".substr($sucursales, 0, -10)."
							) AS b
							LEFT OUTER JOIN (
								SELECT r.Client, r.Branch, $campo_fecha AS FechaInd,
									$operacion($distinto r.`$campo_clave`) AS Valor
								FROM $reporte AS r
								$types
								WHERE $filtros cast(ifnull($campo_fecha, 0) AS Date) BETWEEN $inicio AND $ayer
								AND r.Client = {$this->client}
								GROUP BY r.Branch, FechaInd
							) AS i
							USING (Branch)";
						} else {
							$consulta = "
							INSERT INTO paso
								(Client, Branch, Ind, Kind, FechaInd, Valor)
							SELECT {$this->client}, b.Branch,
								'{$indicador}', '{$fila["Kind"]}',
								i.FechaInd, i.Valor
							FROM (
								".substr($sucursales, 0, -10)."
							) AS b
							LEFT OUTER JOIN (
								SELECT r.Branch, $campo_fecha AS FechaInd,
									$operacion($distinto r.`$campo_clave`) AS Valor
								FROM $reporte AS r
								$types
								WHERE $filtros cast(ifnull($campo_fecha, 0) AS Date) BETWEEN $inicio AND $ayer
								GROUP BY r.Branch, FechaInd
							) AS i
							USING (Branch)";
						}
						if ($this->consulta($consulta))
							$this->mensaje("         ".$this->filas_afectadas." registros insertados\n");
					} else {
						$this->error('No se pudo calcular el indicador ' . $indicador . ' del reporte ' . $reporte . ', se debe revisar su configuracion en galib.', false);
						$this->guardaLog(['01x0101INDI', 'No se pudo calcular el indicador ' . $indicador . ' del reporte ' . $reporte . ', se debe revisar su configuracion en galib.']);
					}
				}
			}
			$resultado->close();

			$this->mensaje("   Separando las fechas...");

			$consulta = "
				UPDATE paso
				SET FechaInd = $ayer
				WHERE ifnull(FechaInd, 0) = 0";
			$this->consulta($consulta);

			$consulta = "
				UPDATE paso
				SET Ano = YEAR(FechaInd),
					Mes = MONTH(FechaInd),
					Dia = DAY(FechaInd),
					Semana = WEEK(FechaInd)";
			$this->consulta($consulta);

			$consulta = "
				UPDATE paso
				SET Valor = 0
				WHERE Valor IS NULL";
			$this->consulta($consulta);

			$this->mensaje("   Marcando indicadores de balance en ceros...");

			$this->eliminaTabla("temp");

			$consulta = "
				CREATE TEMPORARY TABLE temp AS
				SELECT *
				FROM paso
				WHERE Kind = 'B'
					AND FechaInd = $ayer
					AND Valor = 0";
			$this->consulta($consulta);

			$consulta = "
				UPDATE gestion AS g, temp AS t
				SET g.Valor = 0,
					g.Saldo = 0
				WHERE g.Branch = t.Branch AND g.Ind = t.Ind
					AND g.Ano = t.Ano AND g.Mes = t.Mes AND g.Dia = t.Dia";
			$this->consulta($consulta);

			$this
				->mensaje("   Agregando indicadores de Paso a Gestion...")
				->mensaje("      Eliminando indicadores de balance que no pueden estar en cero...");

			$consulta = "
				DELETE FROM paso
				USING paso, galib, tablas
				WHERE paso.Valor = 0
					AND paso.Ind = galib.Id
					AND galib.Reporte = tablas.Name
					AND tablas.Tipo = 'B'
					AND ifnull(tablas.OKReporteVacio, 0) = 0";
			if ($this->consulta($consulta))
				$this->mensaje("         ".$this->filas_afectadas." registros eliminados");

			$this->mensaje("      Agregando registros restantes de Paso a Gestion");

			$consulta = "
				INSERT INTO gestion
					(Client, Branch, Ind, Ano, Mes, Dia, Valor)
				SELECT {$this->client}, p.Branch, p.Ind, p.Ano, p.Mes, p.Dia, p.Valor
				FROM paso AS p
				INNER JOIN 14meses AS m
					USING (Ano, Mes)
				ON DUPLICATE KEY UPDATE Valor = VALUES(Valor)";
			if ($this->consulta($consulta))
				$this->mensaje("         ".$this->filas_afectadas." registros insertados");
		}

		$this->mensaje();

		$this->guardaLog(['02x0102GIND', 'Creacion de indicadores']);

		return $this;
	}

	protected function recalculaSaldos() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102RSAL', 'Recalculo de saldos']);

		$this->identificaCliente();

		$this
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("   Recalculando indicadores de Resultados...");

		for ($i = 1; $i <= 31; $i++) {
			$this->mensaje("      Asignando el Saldo del dia $i...");

			$this->eliminaTabla("temp");

			$consulta = "
				CREATE TEMPORARY TABLE temp AS
				SELECT gt.Branch, gt.Ano, gt.Mes,
					$i AS Dia, gt.Ind, SUM(gt.Valor) AS Valor
				FROM gestion AS gt
				INNER JOIN galib AS gl
					ON gt.Dia <= $i AND gt.Ind = gl.Id
						AND gl.Kind = 'R'
				GROUP BY gt.Branch, gt.Ano, gt.Mes, gt.Ind";
			$this->consulta($consulta);

			$consulta = "
				ALTER TABLE temp
				ADD INDEX _Principal (Branch, Ano, Mes, Ind)";
			$this->consulta($consulta);

			$consulta = "
				UPDATE gestion AS gt, galib AS gl, temp AS t
				SET gt.Saldo = t.Valor
				WHERE gt.Dia = $i
					AND gt.Ind = gl.Id
					AND gl.Kind = 'R'
					AND t.Branch = gt.Branch
					AND t.Ano = gt.Ano
					AND t.Mes = gt.Mes
					AND t.Ind = gt.Ind";
			if ($this->consulta($consulta))
				$this->mensaje("         ".$this->filas_afectadas." saldos actualizados\n");
		}

		$this->mensaje("   Redondeando Valores de Gestion...");

		$consulta = "
			UPDATE gestion AS gt, galib AS gl
			SET gt.Valor = 0
			WHERE gt.Valor < 0.1 AND gt.Valor > -0.1
				AND gt.Ind = gl.Id
				AND gl.Unit = '$'";
		$this->consulta($consulta);

		$consulta = "
			UPDATE gestion
			SET Valor = 0
			WHERE Valor < 0.009 AND Valor > -0.009";
		$this->consulta($consulta);

		$this->mensaje("   Recalculando indicadores de Balance...");

		$consulta = "
			UPDATE gestion AS gt, galib AS gl
			SET gt.Saldo = gt.Valor
			WHERE gt.Ind = gl.Id
				AND gl.Kind = 'B'";
		$this->consulta($consulta);

		$this->mensaje("   Redondeando Saldos de Gestion...");

		$consulta = "
			UPDATE gestion AS gt, galib AS gl
			SET gt.Saldo = 0
			WHERE gt.Saldo < 0.1 AND gt.Saldo > -0.1
				AND gt.Ind = gl.Id
				AND gl.Unit = '$'";
		$this->consulta($consulta);

		$consulta = "
			UPDATE gestion
			SET Saldo = 0
			WHERE Saldo < 0.009 AND Saldo > -0.009";
		$this->consulta($consulta);

		$this->mensaje();

		$this->guardaLog(['02x0102RSAL', 'Recalculo de saldos']);

		return $this;
	}

	protected function agrupaIndicadores() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102AIND', 'Agrupacion de indicadores por marca, division, grupo']);

		$this->identificaCliente();

		$this
			->usarBaseDatos(PREFIJO_SC.$this->client, true)
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("   Agrupando indicadores...");

		if (count($this->sucursales) > 1) {
			$this->mensaje("      Generando total del Grupo...");

			$segmentos = array(
				"SUM" => "<>",
				"AVG" => "=");

			foreach ($segmentos as $funcion => $operador) {
				$this->eliminaTabla("temp");

				$consulta = "
					CREATE TEMPORARY TABLE temp AS
					SELECT gt.Client, 0 AS Branch,
						gt.Ano, gt.Mes, gt.Dia, gt.Ind,
						$funcion(gt.Valor) AS Valor,
						$funcion(gt.Saldo) AS Saldo
					FROM gestion AS gt
					INNER JOIN galib AS gl
						ON gt.Ind = gl.Id
					WHERE gl.Id LIKE 'I%'
						AND ifnull(gl.Estimatecalcvalue, 0) = 0
						AND ifnull(gl.Operacion, '') $operador 'AVERAGE'
					GROUP BY gt.Ano, gt.Mes, gt.Dia, gt.Ind";
				$this->consulta($consulta);

				$consulta = "
					INSERT INTO gestion
						(Client, Branch, Ano, Mes, Dia, Ind, Valor, Saldo)
					SELECT *
					FROM temp";
				$this->consulta($consulta);

				$this->eliminaTabla("temp");

				$consulta = "
					CREATE TEMPORARY TABLE temp AS
					SELECT 0 AS Branch, o.Ind, o.Mes, $funcion(o.Valor) AS Valor
					FROM ".PREFIJO_SC.$this->client.".gaobj AS o
					INNER JOIN galib AS gl
						ON o.Ind = gl.Id
					WHERE gl.Unit $operador '%'
					GROUP BY o.Ind, o.Mes";
				$this->consulta($consulta);

				$consulta = "
					INSERT INTO ".PREFIJO_SC.$this->client.".gaobj
					SELECT *
					FROM temp";
				$this->consulta($consulta);
			}

			if (count($this->marcas) > 1) {
				$this->mensaje("      Generando totales por Marca...");

				foreach ($this->marcas as $marca => $parametros) {
					//if (count($parametros["Sucursales"]) > 1) {
						$this->mensaje("         '{$parametros["Nombre"]}' ($marca)...");

						foreach ($segmentos as $funcion => $operador) {
							$this->eliminaTabla("temp");

							$consulta = "
								CREATE TEMPORARY TABLE temp AS
								SELECT gt.Client, $marca AS Branch,
									gt.Ano, gt.Mes, gt.Dia, gt.Ind,
									$funcion(gt.Valor) AS Valor,
									$funcion(gt.Saldo) AS Saldo
								FROM gestion AS gt
								INNER JOIN galib AS gl
									ON gt.Ind = gl.Id
								WHERE gl.Id LIKE 'I%'
									AND ifnull(gl.Estimatecalcvalue, 0) = 0
									AND ifnull(gl.Operacion, '') $operador 'AVERAGE'
									AND gt.Branch IN (".implode(", ", array_keys($parametros["Sucursales"])).")
								GROUP BY gt.Ano, gt.Mes, gt.Dia, gt.Ind";
							$this->consulta($consulta);

							$consulta = "
								INSERT INTO gestion
									(Client, Branch, Ano, Mes, Dia, Ind, Valor, Saldo)
								SELECT *
								FROM temp";
							$this->consulta($consulta);

							$this->eliminaTabla("temp");

							$consulta = "
								CREATE TEMPORARY TABLE temp AS
								SELECT $marca AS Branch, o.Ind, o.Mes, $funcion(o.Valor) AS Valor
								FROM ".PREFIJO_SC.$this->client.".gaobj AS o
								INNER JOIN galib AS gl
									ON o.Ind = gl.Id
								WHERE gl.Unit $operador '%'
									AND o.Branch IN (".implode(", ", array_keys($parametros["Sucursales"])).")
								GROUP BY o.Ind, o.Mes";
							$this->consulta($consulta);

							$consulta = "
								INSERT INTO ".PREFIJO_SC.$this->client.".gaobj
								SELECT *
								FROM temp";
							$this->consulta($consulta);
						}
					//}
				}
			}

			if (count($this->divisiones)) {
				$this->mensaje("      Generando totales por Division...");

				foreach ($this->divisiones as $division => $parametros) {
					//if (count($parametros["Sucursales"]) > 1) {
						$this->mensaje("         '{$parametros["Nombre"]}' ($division)...");

						foreach ($segmentos as $funcion => $operador) {
							$this->eliminaTabla("temp");

							$consulta = "
								CREATE TEMPORARY TABLE temp AS
								SELECT gt.Client, $division AS Branch,
									gt.Ano, gt.Mes, gt.Dia, gt.Ind,
									$funcion(gt.Valor) AS Valor,
									$funcion(gt.Saldo) AS Saldo
								FROM gestion AS gt
								INNER JOIN galib AS gl
									ON gt.Ind = gl.Id
								WHERE gl.Id LIKE 'I%'
									AND ifnull(gl.Estimatecalcvalue, 0) = 0
									AND ifnull(gl.Operacion, '') $operador 'AVERAGE'
									AND gt.Branch IN (".implode(", ", array_keys($parametros["Sucursales"])).")
								GROUP BY gt.Ano, gt.Mes, gt.Dia, gt.Ind";
							$this->consulta($consulta);

							$consulta = "
								INSERT INTO gestion
									(Client, Branch, Ano, Mes, Dia, Ind, Valor, Saldo)
								SELECT *
								FROM temp";
							$this->consulta($consulta);

							$this->eliminaTabla("temp");

							$consulta = "
								CREATE TEMPORARY TABLE temp AS
								SELECT $division AS Branch, o.Ind, o.Mes, $funcion(o.Valor) AS Valor
								FROM ".PREFIJO_SC.$this->client.".gaobj AS o
								INNER JOIN galib AS gl
									ON o.Ind = gl.Id
								WHERE gl.Unit $operador '%'
									AND o.Branch IN (".implode(", ", array_keys($parametros["Sucursales"])).")
								GROUP BY o.Ind, o.Mes";
							$this->consulta($consulta);

							$consulta = "
								INSERT INTO ".PREFIJO_SC.$this->client.".gaobj
								SELECT *
								FROM temp";
							$this->consulta($consulta);
						}
					//}
				}
			}
		}

		$this->mensaje();

		$this->guardaLog(['02x0102AIND', 'Agrupacion de indicadores por marca, division, grupo']);

		return $this;
	}

	protected function generaGestionT($elimina = false) {
		$this->depurar(__METHOD__);

		if ($elimina)
			$this->eliminaTabla("gestiont");

		$consulta = "
			CREATE TABLE IF NOT EXISTS gestiont AS
			SELECT gt.Client, gt.Branch,
				gt.Ano, gt.Mes, gt.Dia, gt.Ind,
				gt.Valor, gt.Saldo
			FROM gestion AS gt
			INNER JOIN galib AS gl
				ON gt.Ind = gl.Id
			INNER JOIN 14meses AS m
				USING (Ano, Mes)
			WHERE ifnull(gl.Estimatecalcvalue, 0) = 0";
		$this->consulta($consulta);

		if (!$this->existeIndice("Ind", "gestiont")) {
			$consulta = "
				ALTER TABLE gestiont
				ADD INDEX Ind (Ind)";
			$this->consulta($consulta);
		}

		return $this;
	}

	protected function indicadoresCalculados() {
		$this->depurar(__METHOD__);

		$this->guardaLog(['01x0102RSAL', 'Recalculo  de indicadores generados']);

		$this->identificaCliente();

		$this
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("   Recalculando indicadores Calculados...")
			->mensaje("      Generando la base de indicadores I para calcular indicadores C...");

		$this->generaGestionT(true);

		$this->eliminaTabla("tmpdbfo");

		$consulta = "
			CREATE TEMPORARY TABLE tmpdbfo AS
			SELECT *
			FROM gestiont";
		$this->consulta($consulta);

		$consulta = "
			ALTER TABLE tmpdbfo
			ADD INDEX Ind (Ind)";
		$this->consulta($consulta);

		$this->eliminaTabla("tmpform");

		$consulta = "CREATE TABLE tmpform LIKE tmpdbfo";
		$this->consulta($consulta);

		$consulta = null;
		for ($i = 1; $i <= 12; $i++) {
			$consulta .= "
				(SELECT For$i AS Ind
				FROM galib
				WHERE ifnull(Estimatecalcvalue, 0) = 0)
				UNION ALL";
		}

		$consulta = "
			SELECT DISTINCT Ind
			FROM (
				".substr($consulta, 0, -10)."
			) AS i
			WHERE Ind LIKE 'I%' OR
				Ind LIKE 'C%'";
		if ($resultado = $this->consulta($consulta)) {
			$indicadores = array();
			while ($fila = $resultado->fetch_assoc())
				$indicadores[] = $fila["Ind"];
			$resultado->close();

			$this->depurar(print_r($indicadores, true));
		}

		if (count($indicadores)) {
			$this->mensaje("      Identificando indicadores de formula...");

			$this->eliminaTabla("tmpcross");

			$consulta = "
				CREATE TEMPORARY TABLE tmpcross AS
				SELECT Branch, Ano, Mes, Dia, Ind, Saldo AS Valor
				FROM tmpdbfo
				WHERE Ind IN ('".implode("', '", $indicadores)."')";
			$this->consulta($consulta);

			$consulta = "
				ALTER TABLE tmpcross
				ADD INDEX _Principal (Branch, Ano, Mes, Dia, Ind)";
			$this->consulta($consulta);

			$this->eliminaTabla("temp");

			$consulta = "
				CREATE TEMPORARY TABLE temp AS
				SELECT Ind, SUM(Valor) AS ValorSUM
				FROM tmpcross
				GROUP BY Ind
				HAVING ifnull(ValorSUM, 0) = 0";
			$this->consulta($consulta);

			$consulta = "
				ALTER TABLE temp
				ADD INDEX Ind (Ind)";
			$this->consulta($consulta);

			$consulta = "
				DELETE FROM tmpcross
				USING tmpcross, temp
				WHERE tmpcross.Ind = temp.Ind";
			$this->consulta($consulta);

			$consulta = "
				DELETE FROM tmpdbfo
				USING tmpdbfo, temp
				WHERE tmpdbfo.Ind = temp.Ind";
			$this->consulta($consulta);

			$this->mensaje("      Generando Crosstab...");

			$this->eliminaTabla("crostemp");

			$columnas = null;
			foreach ($indicadores as $indicador)
				$columnas .= "SUM(IF(Ind = '$indicador', Valor, 0)) AS $indicador, ";

			$consulta = "
				CREATE TEMPORARY TABLE crostemp AS
				SELECT Branch, Ano, Mes, Dia,
					".substr($columnas, 0, -2)."
				FROM tmpcross
				GROUP BY Branch, Ano, Mes, Dia";
			$this->consulta($consulta);

			$this->eliminaTabla("tmpcross");

			$this->mensaje("      Generando Formulas...");

			$consulta = "
				SELECT DISTINCT Ind
				FROM tmpdbfo
				WHERE Ind LIKE 'I%'";
			if ($resultado = $this->consulta($consulta)) {
				$existentes = array();
				while ($fila = $resultado->fetch_assoc())
					$existentes[] = $fila["Ind"];
				$resultado ->close();

				$no_existentes = array_diff($indicadores, $existentes);
				$this->depurar(print_r($no_existentes, true));
			}

			$this->eliminaTabla("formulas");

			$consulta = "
				CREATE TEMPORARY TABLE formulas AS
				SELECT Id, For1, For2, For3, For4, For5, For6,
					For7, For8, For9, For10, For11, For12
				FROM galib
				WHERE ifnull(Estimatecalcvalue, 0) = 0
					AND ifnull(For1, '') <> ''";
			$this->consulta($consulta);

			if (count($no_existentes))
				for ($i = 1; $i <= 12; $i++) {
					$consulta = "
						DELETE FROM formulas
						WHERE For$i IN ('".implode("', '", $no_existentes)."')";
					$this->consulta($consulta);
				}

			$consulta = "
				SELECT *
				FROM formulas";
			if ($resultado = $this->consulta($consulta)) {
				$indicadores = array();
				while ($fila = $resultado->fetch_assoc()) {
					$this->mensaje("         Calculando formula de '{$fila["Id"]}'...");

					$indicadores[] = $fila["Id"];

					$formula = null;
					for ($i = 1; $i <= 12; $i++) {
						if ($fila["For$i"]) {
							$formula .= $fila["For$i"];
						} else {
							break;
						}
					}
					//$this->depurar = 1;
					$consulta = "
						INSERT INTO tmpform
							(Client, Branch, Ano, Mes, Dia, Ind, Valor)
						SELECT {$this->client}, Branch, Ano, Mes, Dia,
							'{$fila["Id"]}', $formula
						FROM crostemp";
					$this->consulta($consulta);

					$consulta = "
						UPDATE tmpform
						SET Saldo = Valor";
					$this->consulta($consulta);

					$consulta = "
						DELETE FROM gestion
						WHERE Ind  = '{$fila["Id"]}'";
					$this->consulta($consulta);

					$consulta = "
						INSERT INTO gestion
							(Client, Branch, Ano, Mes, Dia, Ind, Valor, Saldo)
						SELECT *
						FROM tmpform";
					$this->consulta($consulta);

					$consulta = "TRUNCATE TABLE tmpform";
					$this->consulta($consulta);
					//$this->depurar = 0;
				}
				$resultado->close();
			}

			$this->mensaje("      Eliminando indicadores sin valor...");

			$this->generaGestionT(true);

			$this->eliminaTabla("temp");

			$consulta = "
				CREATE TEMPORARY TABLE temp
				SELECT Ind, SUM(Valor) AS ValorSUM
				FROM gestiont
				GROUP BY Ind
				HAVING ifnull(ValorSUM, 0) = 0";
			$this->consulta($consulta);

			$consulta = "
				ALTER TABLE temp
				ADD INDEX Ind (Ind)";
			$this->consulta($consulta);

			$consulta = "
				DELETE FROM gestiont
				USING gestiont, temp
				WHERE gestiont.Ind = temp.Ind";
			$this->consulta($consulta);
		}

		$this->mensaje();

		$this->guardaLog(['02x0102RSAL', 'Recalculo  de indicadores generados']);

		return $this;
	}

	protected function exportaDB() {
		$this->depurar(__METHOD__);

		$consulta = "
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = '".PREFIJO_SIM.$this->client."'
		AND TABLE_NAME LIKE 'db%'";

		$resultado = $this->consulta($consulta);

		foreach ($resultado as $key => $tablas) {
			foreach ($tablas as $id => $tabla) {
				$this->guardaLog(['01x0102EASE', 'Exportando ' . $tabla . '...']);
				$this
					->mensaje("            Exportando '$tabla'...")
					->eliminaTabla(PREFIJO_SC.$this->client.".".$tabla);
				$this
					->clonaTabla(PREFIJO_SIM.$this->client.".".$tabla,
						PREFIJO_SC.$this->client.".".$tabla);
				$this->guardaLog(['02x0102EASE', 'Exportando ' . $tabla . '...']);
			}
		}


		return $this;
	}

	protected function exportaGAMonth() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102EGMT', 'Exportando GAMonth...']);

		$this->identificaCliente();

		$this
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("   Exportando GAMonth...");

		$this
			->generaGestionT()
			->eliminaTabla("gamonth");

		$consulta = "
			CREATE TEMPORARY TABLE gamonth AS
			SELECT gt.Client, gt.Branch,
				gt.Ano, gt.Mes, gt.Dia, gt.Ind,
				gt.Valor, gt.Saldo
			FROM gestiont AS gt
			INNER JOIN galib AS gl
				ON gt.Ind = gl.Id
			INNER JOIN 14meses AS m
				USING (Ano, Mes, Dia)
			WHERE ifnull(gl.Estimatecalcvalue, 0) = 0";
		$this->consulta($consulta);

		$this->agregaVirtuals("gamonth");

		$consulta = "
		CREATE TEMPORARY TABLE gamonth2 AS
		SELECT DISTINCT
			Client, Branch, Ano, Mes, Ind, Valor, Saldo
		FROM
			gamonth";
		$this->consulta($consulta);

		$this->eliminaTabla("gamonth");

		$consulta = "ALTER TABLE gamonth2 RENAME gamonth";
		$this->consulta($consulta);

		$consulta = "
			UPDATE gamonth
			SET Valor = Saldo";
		$this->consulta($consulta);

		for ($i = 1; $i <= 12; $i++) {
			$this->mensaje("      Calculando el saldo mensual acumulado del mes $i...");

			$this->eliminaTabla("temp");

			$consulta = "
				CREATE TEMPORARY TABLE temp AS
				SELECT gt.Branch, gt.Ano, $i AS Mes,
					gt.Ind, SUM(gt.Valor) AS Valor
				FROM gamonth AS gt
				INNER JOIN galib AS gl
					ON gt.Ind = gl.Id
				WHERE gt.Mes <= $i
					AND (gl.Kind = 'R'
						OR (gl.Kind = 'B' AND ifnull(gl.Mixtos, '') <> ''))
				GROUP BY gt.Branch, gt.Ano, gt.Ind";
			$this->consulta($consulta);

			$consulta = "
				ALTER TABLE temp
				ADD INDEX _Principal (Branch, Ano, Ind)";
			$this->consulta($consulta);

			$consulta = "
				UPDATE gamonth AS gt, galib AS gl
				SET gt.Saldo = IF(gt.Ano <> ".date("Y", $this->ayer).", 0,
						(SELECT Valor
						FROM temp
						WHERE Branch = gt.Branch
							AND Ano = gt.Ano
							AND Ind = gt.Ind
						LIMIT 1))
				WHERE gt.Mes = $i
					AND gt.Ind = gl.Id
					AND (gl.Kind = 'R'
						OR (gl.Kind = 'B' AND ifnull(gl.Mixtos, '') <> ''))";
			 if ($this->consulta($consulta))
				$this->mensaje("         ".$this->filas_afectadas." saldos actualizados\n");
		}

		$this->clonaTabla("gamonth", PREFIJO_SC.$this->client.".gamonth");

		$this->exportaDBU('gamonth');

		$this->mensaje();

		$this->guardaLog(['02x0102EGMT', 'Exportando GAMonth...']);
		return $this;
	}

	protected function exportaGADay() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102EGDY', 'Exportando GADay...']);
		$this->identificaCliente();

		$this
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("   Exportando GADay...");

		$this
			->generaGestionT()
			->eliminaTabla("gaday");

		$this->clonaTabla("2y2meses","2y2meses_");

		$this->eliminaTabla("2y2meses");

		$consulta = "
		CREATE /*TEMPORARY*/ TABLE 2y2meses AS
		(SELECT * FROM `2y2meses_`
		LIMIT 3 , 2) UNION ALL
		(SELECT * FROM `2y2meses_`
		LIMIT 1 , 1)
		ORDER BY Ano";

		$this->consulta($consulta);

		$consulta = "
			CREATE TABLE gaday AS
			SELECT gt.Client, gt.Branch,
				gt.Ano, gt.Mes, gt.Dia, gt.Ind,
				gt.Valor, gt.Saldo
			FROM gestiont AS gt
			INNER JOIN galib AS gl
				ON gt.Ind = gl.Id
			INNER JOIN 2y2meses AS m
				USING (Ano, Mes)
			WHERE ifnull(gl.Estimatecalcvalue, 0) = 0";
		$this->consulta($consulta);

		$this->agregaVirtuals("gaday");

		$consulta = "
		CREATE TEMPORARY TABLE gaday2 AS
		SELECT DISTINCT
			Client, Branch, Ano, Mes, Dia, Ind, Valor, Saldo
		FROM
			gaday";
		$this->consulta($consulta);

		$this->eliminaTabla("gaday");

		$consulta = "ALTER TABLE gaday2 RENAME gaday";
		$this->consulta($consulta);

		$this->clonaTabla("gaday", PREFIJO_SC.$this->client.".gaday");

		$this->exportaDBU('gaday');

		$this->mensaje();

		$this->guardaLog(['02x0102EGDY', 'Exportando GADay...']);

		return $this;
	}

	protected function preparavirtuals(){
		$this->depurar(__METHOD__);

		$this->mensaje("   Prepara virtuals");

		$this->generaGestionT(true);

		$this->eliminaTabla("virtuals");

		$consulta = "
		CREATE TABLE virtuals AS
		SELECT *  FROM gestiont
		WHERE (Branch = 0 OR Branch >= 1999)
			AND Ano >= ".$this->ano_final;
		$this->consulta($consulta);

		$this->mensaje("      Eliminando Branch 0");
		$consulta = "
			DELETE FROM gestion
			WHERE (IFNULL(Branch, 0) = 0
				AND Ano >= ".$this->ano_final.")";
		if ($this->consulta($consulta))
			$this->mensaje("         ".$this->filas_afectadas." registros eliminados");

		$this->mensaje("      Eliminando Branch 1999, 7000");
		$consulta = "
			DELETE FROM gestion
			WHERE (Branch >= 1999
				AND Ano >= ".$this->ano_final.")";
		if ($this->consulta($consulta))
			$this->mensaje("         ".$this->filas_afectadas." registros eliminados");

		if ($this->existeTabla("tmpform")){
			$consulta = "DELETE FROM tmpform WHERE Ind NOT LIKE 'C%'";
			$this->consulta($consulta);

			$consulta = "UPDATE tmpform SET Client = '".$this->client."'";
			$this->consulta($consulta);

			$consulta = "CREATE TEMPORARY TABLE tmpdelete AS
				SELECT * FROM tmpform WHERE Branch = 0 OR Branch >= 1999";
			$this->consulta($consulta);

			$consulta = "DELETE FROM tmpform WHERE IFNULL(Branch, 0) = 0 OR Branch >= 1999";
			$this->consulta($consulta);

				if ($this->existeTabla("virtuals")) {
					$this->fusionaTablas("tmpdelete","virtuals");
				} else {
					$consulta = "ALTER TABLE tmpdelete RENAME virtuals";
					$this->consulta($consulta);
				}

			$tabla = ($this->existeTabla("4meses")) ? "4meses" : "14meses";

			$consulta = "CREATE TEMPORARY TABLE deltmpform AS
				SELECT t.Client, t.Branch, t.Ind, t.Ano, t.Mes, t.Dia, t.Valor, t.Saldo
				FROM `tmpform` AS t
				INNER JOIN `{$tabla}` AS m ON t.Ano = m.Ano AND t.Mes = m.Mes";
			$this->consulta($consulta);

			$this->mensaje("   Agrega los indicadores calculados a GESTION");

			$consulta = "INSERT INTO gestion (Client, Branch, Ind, Ano, Mes, Dia, Valor, Saldo)
				SELECT Client, Branch, Ind, Ano, Mes, Dia, Valor, Saldo
				FROM tmpform
				ON DUPLICATE KEY UPDATE Valor=VALUES(Valor), Saldo=VALUES(Saldo)";
			$this->consulta($consulta);
			$this->mensaje("         ".$this->filas_afectadas." registros insertados\n");
		}

		return $this;
	}

	protected function agregaVirtuals($tabla){
		$this->depurar(__METHOD__);

		if($this->existeTabla("virtuals")) {

			$this->mensaje("      Agregando registros Virtuales a  ".strtoupper($tabla)."...");

			$this->eliminaTabla("temp{$tabla}");

			$consulta = "CREATE /*TEMPORARY*/ TABLE temp{$tabla} AS
			SELECT
				m.Ano, m.Mes, m.Dia, v.Ind
			FROM
				`14meses` AS m
			  INNER JOIN
					`virtuals` AS v ON m.Ano = v.Ano AND m.Mes = v.Mes AND m.Dia = v.Dia
			  INNER JOIN
					`galib` AS g ON v.Ind = g.Id AND g.Estimatecalcvalue IS NULL";
			$this->consulta($consulta);

			$consulta = "
			CREATE TEMPORARY TABLE tmpvt{$tabla} AS
			SELECT DISTINCT *
			FROM temp{$tabla}";
			$this->consulta($consulta);

			$this->eliminaTabla("temp{$tabla}");

			$consulta = "ALTER TABLE tmpvt{$tabla} RENAME TO temp{$tabla}";
			$this->consulta($consulta);

			$this->fusionaTablas("temp{$tabla}","{$tabla}");

			$this->mensaje("         ".$this->filas_afectadas." registros insertados\n");

		} else {
			$this->mensaje("      No existe la tabla 'Virtuals'...");
		}

		return $this;
	}

	protected function exportaGADate() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102EGDY', 'Exportando GADate...']);
		$this->identificaCliente();

		$this
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("   Exportando GADate...");

		$this->eliminaTabla(PREFIJO_SC.$this->client.".gadate");

		$consulta = "
			CREATE TABLE ".PREFIJO_SC.$this->client.".gadate AS
			SELECT *
			FROM 2y2meses
			ORDER BY Dia, Mes, Ano
			LIMIT 2";

		$this->consulta($consulta);

		$this->eliminaTabla("2y2meses");

		$this->mensaje();

		$this->guardaLog(['02x0102EGDY', 'Exportando GADate...']);

		return $this;
	}

	protected function exportaGALib() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102EGLB', 'Exportando GAlib...']);
		$this->identificaCliente();

		$this
			->usarBaseDatos(PREFIJO_SIM.$this->client, true)
			->mensaje("   Exportando GALib...")
			->mensaje("      Eliminando indicadores sin datos en 14 meses...");

		$this->eliminaTabla("temp");

		$consulta = "
			CREATE TEMPORARY TABLE temp AS
			SELECT Ind, SUM(Saldo) AS SaldoSUM
			FROM gestion
			GROUP BY Ind";
		$this->consulta($consulta);

		$consulta = "
			DELETE FROM temp
			WHERE ifnull(SaldoSUM, 0) <> 0";
		$this->consulta($consulta);

		$consulta = "
			ALTER TABLE temp
			ADD INDEX Ind (Ind)";
		$this->consulta($consulta);

		$consulta = "
			DELETE FROM gestion
			USING gestion, temp
			WHERE gestion.Ind = temp.Ind";
		if ($this->consulta($consulta))
			$this->mensaje("         ".$this->filas_afectadas." registros eliminados");

		$consulta = "
			DELETE FROM ".PREFIJO_SC.$this->client.".galib
			USING ".PREFIJO_SC.$this->client.".galib, temp
			WHERE ".PREFIJO_SC.$this->client.".galib.Id = temp.Ind";
		if ($this->consulta($consulta))
			$this->mensaje("         ".$this->filas_afectadas." indicadores eliminados");

		$this->mensaje("      Eliminando departamentos sin indicadores...");

		$this->usarBaseDatos(PREFIJO_SC.$this->client);

		$this->eliminaTabla("temp");

		$consulta = "
			CREATE TEMPORARY TABLE temp AS
			SELECT Dept, count(*) AS Cantidad
			FROM galib
			HAVING Cantidad = 3";
		$this->consulta($consulta);

		$consulta = "
			DELETE FROM galib
			WHERE Dept IN (SELECT Dept FROM temp)";
		if ($this->consulta($consulta))
			$this->mensaje("         ".$this->filas_afectadas." indicadores eliminados");

		$this->eliminaTabla("temp");

		$consulta = "
			CREATE TEMPORARY TABLE temp AS
			SELECT gl.No, gl.Id, gl.Dept, gl.Bsc, gl.Unit,
				gl.Sign, gl.Kind, gl.AcctSign AS DB, gl.For1,
				gl.For2, gl.For3, gl.For4, gl.For5, gl.For6,
				gl.For7, gl.For8, gl.For9, gl.For10, gl.For11,
				gl.For12,gl.Benchmark, gl.Name AS Espanol, gl.English,
				gl.Mixtos AS Balanza, gl.Finmes, gl.Reporte,
				gl.Campofiltro1, gl.Filtro1, gl.Campofiltro2, gl.Filtro2,
				gl.Campofiltro3, gl.Filtro3, gl.Campofiltro4, gl.Filtro4,
				gl.Campofiltro5, gl.Filtro5, gl.Campotype, gl.Type,
				gl.Campoclave, gl.Operacion, gl.Campofecha,
				gl.Roambi
			FROM galib AS gl
			INNER JOIN gamonth AS gm
				ON gl.Id = gm.Ind OR gl.Id LIKE 'H%'
			WHERE ifnull(gl.Estimatecalcvalue, 0) = 0
			GROUP BY gl.Id
			ORDER BY gl.Dept, Espanol";
		$this->consulta($consulta);

		$this->eliminaTabla("galib");

		$this->clonaTabla("temp", "galib");

		$this->mensaje("      Modificando indicadores para visualizar tablas de personal activo...");

		// ToDo: Actualizar los KPIs de Balance que vienen de un reporte de Resultados

		$this->mensaje();

		$this->guardaLog(['02x0102EGLB', 'Exportando GAlib...']);

		return $this;
	}

	protected function exportaGAPoints() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102EGPT', 'Exportando GAPoints...']);

		$this
			->usarBaseDatos(PREFIJO_SC.$this->client, true)
			->mensaje("   Exportando GAPoints...");

		if ($this->existeTabla("gapoints")){

			$consulta = "
				INSERT INTO gapoints (Branch, Ind)
				(SELECT
				    Branch,Id
				FROM
				    ".PREFIJO_SIM.$this->client.".galib AS t1
				        CROSS JOIN
				    gabranch AS t2
				WHERE
				    t1.id LIKE '%I%' OR t1.Id LIKE '%C%')";
			$this->consulta($consulta);
			$this->mensaje("         ".$this->filas_afectadas." registros insertados");

			$consulta = "
				UPDATE gapoints
				SET
					`D#` = '0',
					`D%` = '0',
					`I#` = '0',
					`I%` = '0',
					`Pos` = '0.5',
					`Neg` = '0.5',
					`Points` = '0'
				WHERE
					`D#` IS NULL
					AND `D%` IS NULL
					AND `I#` IS NULL
					AND `I%` IS NULL
					AND `Pos` IS NULL
					AND `Neg` IS NULL
					AND `Points` IS NULL";
			$this->consulta($consulta);
			$this->mensaje("         ".$this->filas_afectadas." registros actualizados");

			if ($this->existeTabla(PREFIJO_SIM.$this->client.".puntos")){
				$this->mensaje("      Cruzando con 'Puntos'...");
				$consulta = "
					UPDATE gapoints AS gp,
					".PREFIJO_SIM.$this->client.".puntos AS pt
					SET
						gp.`D#` = pt.`D#`,
						gp.`D%` = pt.`D%`,
						gp.`I#` = pt.`I#`,
						gp.`I%` = pt.`I%`,
						gp.`Pos` = pt.`Pos`,
						gp.`Neg` = pt.`Neg`,
						gp.`Points` = pt.`Points`
					WHERE gp.Branch = pt.Branch
					AND gp.Ind = pt.Ind";
				$this->consulta($consulta);

				$this->mensaje("         ".$this->filas_afectadas." registros actualizados");

				$this->mensaje("      Agrega suma de puntos al Branch 0...");
				$consulta = "INSERT INTO gapoints
							SELECT '0' AS Branch, Ind, `D#`, `D%`, `I#`, `I%`,
								AVG(Pos) AS Pos, AVG(Neg) AS Neg, SUM(Points) AS Points
							FROM gapoints
							GROUP BY IFNULL(`Ind`,0), IFNULL(`D#`,0), IFNULL(`D%`,0), IFNULL(`I#`,0), IFNULL(`I%`,0)";
				$this->consulta($consulta);
				$this->mensaje("         ".$this->filas_afectadas." registros actualizados");

				if (count($this->marcas) > 1) {
					$this->mensaje("      Generando totales por Marca...");

					foreach ($this->marcas as $marca => $parametros) {
						$this->mensaje("         '{$parametros["Nombre"]}' ($marca)...");
						$consulta = "INSERT INTO gapoints
									SELECT {$marca} AS Branch, Ind, `D#`, `D%`, `I#`, `I%`,
										AVG(Pos) AS Pos, AVG(Neg) AS Neg, SUM(Points) AS Points
									FROM gapoints
									WHERE Branch IN(".implode(", ", array_keys($parametros["Sucursales"])).")
									GROUP BY IFNULL(`Ind`,0), IFNULL(`D#`,0), IFNULL(`D%`,0), IFNULL(`I#`,0), IFNULL(`I%`,0)";
						$this->consulta($consulta);
						$this->mensaje("         ".$this->filas_afectadas." registros actualizados");
					}
				}

				if (count($this->divisiones)) {
					$this->mensaje("      Generando totales por Division...");

					foreach ($this->divisiones as $division => $parametros) {
						$this->mensaje("         '{$parametros["Nombre"]}' ($division)...");
						$consulta = "INSERT INTO gapoints
									SELECT {$division} AS Branch, Ind, `D#`, `D%`, `I#`, `I%`,
										AVG(Pos) AS Pos, AVG(Neg) AS Neg, SUM(Points) AS Points
									FROM gapoints
									WHERE Branch IN(".implode(", ", array_keys($parametros["Sucursales"])).")
									GROUP BY IFNULL(`Ind`,0), IFNULL(`D#`,0), IFNULL(`D%`,0), IFNULL(`I#`,0), IFNULL(`I%`,0)";
						$this->consulta($consulta);
						$this->mensaje("         ".$this->filas_afectadas." registros actualizados");
					}
				}

			}


		} else {
			$this->mensaje("      Error al generar GAPoints, la tabla no existe...");
			$this->guardaLog(['01x0101DBTB', 'No se encuentra la tabla GAPoints...']);
		}

		$this->mensaje();

		$this->guardaLog(['02x0102EGPT', 'Exportando GAPoints...']);

		return $this;
	}
	/*
	 *
	 * F1  = Cierre al Dia de Ayer
	 * F2  = Mismo dia Mes Anterior
	 * F3  = Cierre del Mes Anterior
	 * F4  = Mismo dia A¤o Anterior
	 * F5  = Cierre hace 2 Meses
	 * F6  = Cierre Mes Anterior A¤o Pasado
	 * F7  = Objetivo Mes Actual
	 * F8  = Objetivo Mes Cierre
	 * F9  = Puntos
	 * F10 = Cierre de Este mes A¤o Pasado
	 * F11 = Tendencia
	 * F12 = Score
	 * F13 = Promedio de los ultimos 13 meses
	 * F14 = Acumulado al A¤o

	 * F15 = Puntos Ganados al Dia
	 * F16 = Puntos Ganados al Cierre

	 * F17 = Speed (Calcula el avance del mes vs el cierre del mes pasado
	 *              y lo compara contra el promedio del mercado. Con esto
	 *              podemos identificar cuando una sucursal esta creciendo
	 *              pero comparado con el mercado, esta creciendo menos.
	 *
	 * F18 = Mismo Dia Habil Mes Anterior
	 * F19 = Mismo Dia Habil Ano Anterior
	 * F20 = Best Month de los 14
	 * F21 = Porcentaje al dia vs el mejor mes
	 * F22 = Porcentaje al cierre anterior vs mejor mes
	 */

	protected function exportaIndMfr($fabricante) {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102EDMF', 'Exportando IndMfr...']);

		$this->mensaje("      Exportando IndMfr...");
		$basedatos = $this->baseDatosActual();

		$numPlantas = count($this->marcas);
		$numErrores = 0;
		foreach ($this->marcas as $marca => $parametros){
			$planta = strtolower($parametros["Nombre"]);
			$this->usarBaseDatos("mfr_$planta",true);

			if ($this->existeTabla("galib")){

				$consulta = "
					UPDATE ".PREFIJO_SC.$this->client.".indmfr AS im, mfr_$planta.galib AS gl
					SET im.".strtoupper($planta)." = '1'
					WHERE im.Id = gl.Id";
				$this->consulta($consulta);

				$consulta = "
					UPDATE ".PREFIJO_SC.$this->client.".indmfr
					SET ".strtoupper($planta)." = 0
					WHERE ".strtoupper($planta)." IS NULL	";
				$this->consulta($consulta);


				$this->mensaje("         '$planta'...");

			} else {
				$numErrores++;
				$this->mensaje("         No existe base de datos para el fabricante '$planta'");
				$this->guardaLog(['01x0101EIMF', 'No existe base de datos para el fabricante ' . $planta]);
			}

		}

		$this->depurar("Numero de Plantas: ".$numPlantas." Numero de Errores: ".$numErrores);

		if ($numPlantas == $numErrores) {
			$this->eliminaTabla("indmfr");
			$this
				->mensaje("            No existe datos suficientes para exporta IndMfr...")
				->guardaLog(['01x0101EIMF', 'No existe datos suficientes para exporta IndMfr']);
		}

		$this->mensaje();

		$this->usarBaseDatos($basedatos);

		$this->guardaLog(['02x0102EDMF', 'Exportando IndMfr...']);

		return $this;
	}

	protected function exportaIphone() {
		$this->depurar(__METHOD__);

		$this->guardaLog(['01x0102EIPH', 'Exportacion de Iphone']);

		$this->identificaCliente();

		$this
			->usarBaseDatos(PREFIJO_SC.$this->client, true)
			->mensaje("   Exportando iPhone...");

		$f0 = explode("-", date("Y-m-01", $this->ayer));

		$f1 = explode("-", date("Y-m-d", $this->ayer));

		$f2 = explode("-", date("Y-m-d", strtotime("-1 month", $this->ayer)));
		if ($f1[1] - 1 > 0)
			while ($f2[1] > ($f1[1] - 1))
				$f2 = explode("-", date("Y-m-d", strtotime("-1 day", strtotime(implode("-", $f2)))));

		$f3 = explode("-", date("Y-m-d", strtotime("-1 day", strtotime("first day of this month", $this->ayer))));

		$f4 = explode("-", date("Y-m-d", strtotime("-1 year", $this->ayer)));
		while ($f4[1] != $f1[1])
			$f4 = explode("-", date("Y-m-d", strtotime("-1 day", strtotime(implode("-", $f4)))));

		$f5 = explode("-", date("Y-m-d", strtotime("-1 day", strtotime("-1 month", strtotime("first day of this month", $this->ayer)))));

		$f6 = explode("-", date("Y-m-d", strtotime("-1 day", strtotime("-1 year", strtotime("first day of this month", $this->ayer)))));

		$f10 = explode("-", date("Y-m-d", strtotime("-1 day", strtotime("-1 year", strtotime("+1 month", strtotime("first day of this month", $this->ayer))))));

		$d = (int)(round((strtotime(implode("-", $f1)) - strtotime(implode("-", $f0))) / (24 * 60 * 60)) + 1);
		$dom = (int)(($d + ((8 + date("w", strtotime(implode("-", $f0))) - 1) % 8) + 1) / 7);
		$dh = $d - $dom;

		$consulta = "
			SELECT *
			FROM indice.feriados
			WHERE Fecha BETWEEN ".implode("", $f0)." AND ".implode("", $f1)."
				AND Pais = '{$this->pais}'";
		if ($this->consulta($consulta))
			$dh -= $this->filas_afectadas;

		$f18 = explode("-", date("Y-m-d", strtotime("-1 day", strtotime("+".(int)(($dh*7)/6)."day", strtotime("{$f2[0]}-{$f2[1]}-01")))));
		$d2 = (int)round((strtotime(implode("-", $f18)) - strtotime("{$f18[0]}-{$f18[1]}-01")) / (24 * 60 * 60));

		$consulta = "
			SELECT *
			FROM indice.feriados
			WHERE Fecha BETWEEN {$f18[0]}{$f18[1]}01 AND ".implode("", $f18)."
				AND Pais = '{$this->pais}'";
		if ($this->consulta($consulta))
			$d2 -= $this->filas_afectadas;

		$f18 = explode("-", date("Y-m-d", strtotime("+$d2 day", strtotime("{$f18[0]}-{$f18[1]}-01"))));
		// TODO: Descomentar para que no se tome Domingo como día hábil
		/* if ((int)date("w", strtotime(implode("-", $f18))) == 0)
			$f18[2] -= 1; */

		$f19 = explode("-", date("Y-m-d", strtotime("-1 day", strtotime("+".(int)(($dh*7)/6)."day", strtotime("{$f4[0]}-{$f4[1]}-01")))));
		$d4 = (int)round((strtotime(implode("-", $f19)) - strtotime("{$f19[0]}-{$f19[1]}-01")) / (24 * 60 * 60));

		$consulta = "
			SELECT *
			FROM indice.feriados
			WHERE Fecha BETWEEN {$f19[0]}{$f19[1]}01 AND ".implode("", $f19)."
				AND Pais = '{$this->pais}'";
		if ($this->consulta($consulta))
			$d4 -= $this->filas_afectadas;

		$f19 = explode("-", date("Y-m-d", strtotime("+$d4 day", strtotime("{$f19[0]}-{$f19[1]}-01"))));
		// TODO: Descomentar para que no se tome Domingo como día hábil
		/* if ((int)date("w", strtotime(implode("-", $f19))) == 0)
			$f19[2] -= 1; */

		$fm = explode("-", date("Y-m-d", strtotime("-1 day", strtotime("+1 month", strtotime(implode("-", $f0))))));
		$d = (int)(round((strtotime(implode("-", $fm)) - strtotime(implode("-", $f0))) / (24 * 60 * 60)) + 1);
		$p = date("d", $this->ayer) / $d;

		$this->eliminaTabla("tempind");

		$consulta = "
			CREATE TEMPORARY TABLE tempind AS
			SELECT `Id`, `Dept`, `Sign`, `Espanol` as Name, `English`, `Unit`, `Kind`, `DB`
			FROM galib
			WHERE Id NOT LIKE 'H%'";
		$this->consulta($consulta);

		$this->eliminaTabla("iphone");

		$consulta = "
			CREATE TABLE iphone AS
			SELECT {$this->client} as Client,  Branch, Dept, Name, English, Id, Sign, Unit, Kind,
				F1, F2, F3, F4, F5, F6, F7, F8, F9, F10,
				IF(F6 <> 0,
					IF(NOT (((F10 / F6) > 1.35) OR ((F10 / F6) < 0.65)),
						IF(F6 <> 0, (((F10 / F6) * F3) * (1 - $p)), 0) +
						IF(F2 <> 0, (((F1 * F3) / F2) * $p), 0), 0), 0) AS F11,
				F12, F13, F14, F15, F16, F1 / F3 AS F17, F18, F19, F20,
				F1 / F20 * Sign * 100 AS F21, F3 / F20 * Sign * 100 AS F22,
				`Date`
			FROM (
				SELECT Branch, Dept, Name, English, Id, Sign, Unit, Kind,
                    IF(DB = -1 AND Kind = 'R', (SUM(IF(Campo = 'F1', Valor, NULL)) * -1), SUM(IF(Campo = 'F1', Valor, NULL))) AS F1,
                    IF(DB = -1 AND Kind = 'R', (SUM(IF(Campo = 'F2', Valor, NULL)) * -1), SUM(IF(Campo = 'F2', Valor, NULL))) AS F2,
                    IF(DB = -1 AND Kind = 'R', (SUM(IF(Campo = 'F3', Valor, NULL)) * -1), SUM(IF(Campo = 'F3', Valor, NULL))) AS F3,
                    IF(DB = -1 AND Kind = 'R', (SUM(IF(Campo = 'F4', Valor, NULL)) * -1), SUM(IF(Campo = 'F4', Valor, NULL))) AS F4,
                    IF(DB = -1 AND Kind = 'R', (SUM(IF(Campo = 'F5', Valor, NULL)) * -1), SUM(IF(Campo = 'F5', Valor, NULL))) AS F5,
                    IF(DB = -1 AND Kind = 'R', (SUM(IF(Campo = 'F6', Valor, NULL)) * -1), SUM(IF(Campo = 'F6', Valor, NULL))) AS F6,
					SUM(IF(Campo = 'F7', Valor, NULL)) AS F7,
					SUM(IF(Campo = 'F8', Valor, NULL)) AS F8,
					SUM(IF(Campo = 'F9', Valor, NULL)) AS F9,
					SUM(IF(Campo = 'F10', Valor, NULL)) AS F10,
                    IF(DB = -1 AND Kind = 'R', (SUM(IF(Campo = 'F12', Valor, NULL)) * -1), SUM(IF(Campo = 'F12', Valor, NULL))) AS F12,
                    IF(DB = -1 AND Kind = 'R', (SUM(IF(Campo = 'F13', Valor, NULL)) * -1), SUM(IF(Campo = 'F13', Valor, NULL))) AS F13,
                    IF(DB = -1 AND Kind = 'R', (SUM(IF(Campo = 'F14', Valor, NULL)) * -1), SUM(IF(Campo = 'F14', Valor, NULL))) AS F14,
					NULL AS F15,
					NULL AS F16,
					NULL AS F17,
                    IF(DB = -1 AND Kind = 'R', (SUM(IF(Campo = 'F18', Valor, NULL)) * -1), SUM(IF(Campo = 'F18', Valor, NULL))) AS F18,
                    IF(DB = -1 AND Kind = 'R', (SUM(IF(Campo = 'F19', Valor, NULL)) * -1), SUM(IF(Campo = 'F19', Valor, NULL))) AS F19,
					SUM(IF(Campo = 'F20', Valor, NULL)) AS F20,
					CAST(".date("Ymd", $this->ayer)." AS Date) AS `Date`
				FROM (
					(SELECT Branch, Ind,
						'F1' AS Campo, Saldo AS Valor
					FROM gaday
					WHERE Ano = {$f1[0]}
						AND Mes = {$f1[1]}
						AND Dia = {$f1[2]})
					UNION ALL
					(SELECT Branch, Ind,
						'F2' AS Campo, Saldo AS Valor
					FROM gaday
					WHERE Ano = {$f2[0]}
						AND Mes = {$f2[1]}
						AND Dia = {$f2[2]})
					UNION ALL
					(SELECT Branch, Ind,
						'F3' AS Campo, Valor
					FROM gamonth
					WHERE Ano = {$f3[0]}
						AND Mes = {$f3[1]})
					UNION ALL
					(SELECT Branch, Ind,
						'F4' AS Campo, Saldo AS Valor
					FROM gaday
					WHERE Ano = {$f4[0]}
						AND Mes = {$f4[1]}
						AND Dia = {$f4[2]})
					UNION ALL
					(SELECT Branch, Ind,
						'F5' AS Campo, Valor
					FROM gamonth
					WHERE Ano = {$f5[0]}
						AND Mes = {$f5[1]})
					UNION ALL
					(SELECT Branch, Ind,
						'F6' AS Campo, Valor
					FROM gamonth
					WHERE Ano = {$f6[0]}
						AND Mes = {$f6[1]})
					UNION ALL
					(SELECT Branch, Ind,
						'F7' AS Campo, Valor
					FROM gaobj
					WHERE Mes = {$f1[1]})
					UNION ALL
					(SELECT Branch, Ind,
						'F8' AS Campo, Valor
					FROM gaobj
					WHERE Mes = {$f3[1]})
					UNION ALL
					(SELECT Branch, Ind,
						'F9' AS Campo, Points AS Valor
					FROM gapoints)
					UNION ALL
					(SELECT Branch, Ind,
						'F10' AS Campo, Valor
					FROM gamonth
					WHERE Ano = {$f10[0]}
						AND Mes = {$f10[1]})
					UNION ALL
					(SELECT Branch, Ind,
						'F18' AS Campo, Saldo AS Valor
					FROM gaday
					WHERE Ano = {$f18[0]}
						AND Mes = {$f18[1]}
						AND Dia = {$f18[2]})
					UNION ALL
					(SELECT Branch, Ind,
						'F19' AS Campo, Saldo AS Valor
					FROM gaday
					WHERE Ano = {$f19[0]}
						AND Mes = {$f19[1]}
						AND Dia = {$f19[2]})
					UNION ALL
					(SELECT gt.Branch, gt.Ind,
						'F20' AS Campo,
                        IF(gl.Sign = 1, MAX(gt.Valor),
                            IF(gl.DB = -1 AND gl.Kind = 'R', (MAX(gt.Valor) * -1),
                                IF(gl.Sign = -1, MIN(gt.Valor), 0))) AS Valor
					FROM gamonth AS gt
					INNER JOIN galib AS gl
						ON gt.Ind = gl.Id
					WHERE NOT (gt.Ano = {$f1[0]} AND gt.Mes = {$f1[1]})
					GROUP BY gt.Branch, gt.Ind)
				) AS d
				INNER JOIN tempind AS t
					ON d.Ind = t.Id
				GROUP BY Branch, Ind
			) AS i";
		$this->consulta($consulta);
		$this->calculaFQuinceDieciseis();

        $this->mensaje("      Cambiando signos...");

		$consulta = "
			UPDATE gamonth, galib
			SET gamonth.Valor = gamonth.Valor * -1,
				gamonth.Saldo = gamonth.Saldo * -1
			WHERE gamonth.Ind = galib.Id
				AND galib.Kind = 'R'
				AND galib.DB = -1";
		$this->consulta($consulta);

		$consulta = "
			UPDATE gaday, galib
			SET gaday.Valor = gaday.Valor * -1,
				gaday.Saldo = gaday.Saldo * -1
			WHERE gaday.Ind = galib.Id
				AND galib.Kind = 'R'
				AND galib.DB = -1";
		$this->consulta($consulta);

		$this->mensaje("      Calculando Score...");

		$consulta = "
			UPDATE iphone AS i,
				(SELECT gt.Branch, gt.Ind,
					sum(if(NOT (gt.Ano = {$f1[0]} AND gt.Mes = {$f1[1]}), 1, 0)) as Meses,
					std(if(NOT (gt.Ano = {$f1[0]} AND gt.Mes = {$f1[1]}), gt.Valor, NULL)) AS StDv,
					avg(if(NOT (gt.Ano = {$f1[0]} AND gt.Mes = {$f1[1]}), gt.Valor, NULL)) AS Prom,
					sum(if(gt.Ano = {$f1[0]} AND gl.Kind <> 'B', gt.Valor, 0)) AS Acum
				FROM gamonth AS gt
				INNER JOIN galib AS gl
					ON gt.Ind = gl.Id
				WHERE ifnull(gt.Valor, 0) <> 0
				GROUP BY gt.Branch, gt.Ind
				HAVING Meses = 13) AS s
			SET i.F12 = s.StDv / s.Prom,
				i.F13 = s.Prom,
				i.F14 = s.Acum
			WHERE i.Branch = s.Branch
				AND i.Id = s.Ind";
		if ($this->consulta($consulta))
			$this->mensaje("         ".$this->filas_afectadas." indicadores procesados");

		$this->mensaje("      Actualizando etiquetas...");

		$consulta = "
			UPDATE galabels
			SET Mexico = 'Dias Habiles',
				Ingles = $dh
			WHERE Indice = 32";
		$this->consulta($consulta);

		$consulta = "
			UPDATE galabels
			SET Mexico = 'Mismo Dia Habil Ano Anterior',
				Ingles = '".date("m/d/Y", strtotime(implode("-", $f19)))."'
			WHERE Indice = 33";
		$this->consulta($consulta);

		$consulta = "
			UPDATE galabels
			SET Mexico = 'Mismo Dia Habil Mes Anterior',
				Ingles = '".date("m/d/Y", strtotime(implode("-", $f18)))."'
			WHERE Indice = 34";
		$this->consulta($consulta);

		$this->mensaje("      Replicando iPhone a Master...");

		$this->eliminaTabla("master");

		$this->clonaTabla("iphone", "master");

		$this->exportaDBU('iphone');

		foreach (array($f0, $f1, $f2, $f3, $f4, $f5, $f6, $f10, $f18, $f19, $fm, $d, $p, $dom, $dh, $d2, $d4) as $f)
			$this->depurar(is_array($f) ? implode("-", $f) : $f);

		$this->mensaje();

		$this->guardaLog(['02x0102EIPH', 'Exportacion de Iphone']);

        // Se agrega el redondeo de gaday y gamonth y se retira de los métodos exportaGaday y exportaGamonth
        $consulta = "
            UPDATE gaday
            SET Valor = 0
            WHERE Valor < 0.009 AND Valor > -0.009";
        $this->consulta($consulta);

        $consulta = "
            UPDATE gaday
            SET Saldo = 0
            WHERE Saldo < 0.009 AND Saldo > -0.009";
        $this->consulta($consulta);

		return $this;
	}

	/**
	 * Método que calcula el F15 y F16 de la tabla iphone
	 *
	 */
	protected function calculaFQuinceDieciseis() {
		$this->depurar(__METHOD__);

		$this->identificaCliente();

		$this
			->usarBaseDatos(PREFIJO_SC.$this->client, true)
			->mensaje("   Calcula Puntos...");

		/**
		 * Identifica si hay puntos por calcular
		 *
		 */

		$consulta = "
			CREATE TABLE tmppunt
			SELECT Branch, Id, Sign, F1, F3, F7, F8, F9, F1 / F7 as PDia, F3 / F8 as PMes
			FROM iphone where F7 not like '0' and F8 not like '0' and F9 not like '0'
			GROUP BY  Branch, Id, Sign, F1, F3, F7, F8, F9";
		$this->consulta($consulta);

		$consulta = "
			ALTER TABLE tmppunt
			ADD I decimal(15,14),
			ADD D decimal(15,4),
			ADD GDia decimal(59,4),
			ADD GMes decimal(59,4)";
		$this->consulta($consulta);

		$consulta = "
			UPDATE tmppunt as tp,
			gapoints as g
			SET
				tp.D = g.`D%`,
				tp.I = g.`I%`
			WHERE
				tp.Branch = g.Branch
				AND tp.Id = g.Ind";
		$this->consulta($consulta);

		$this->mensaje("   Calcula puntos ganados de indicadores positivos...");
		/**
		 * Los puntos puntos positivos ganados son los puntos elegibles por el porcentaje
		 * de Puntos Ganados del día o del cierre de mes
		 */
		$consulta = "
			UPDATE tmppunt as tp
			SET
				tp.GDia = tp.PDia * tp.F9,
				tp.GMes = tp.PMes * tp.F9
			WHERE
				tp.Sign = 1";
		$this->consulta($consulta);

		/**
		* Sin embargo hay excepciones. Por ejemplo cuando el porcentaje de avance es menos al minimo para calculo
		* entonces los puntos ganados = 0
		*/
		$consulta = "
		UPDATE tmppunt as tp
		SET
		   tp.GDia = 0
	   	WHERE
		   tp.PDia < tp.D
		   AND tp.Sign = 1";
		$this->consulta($consulta);

		$consulta = "
		UPDATE tmppunt as tp
		SET
			tp.GMes = 0
		WHERE
			tp.PMes < tp.D
			AND tp.Sign = 1";
		$this->consulta($consulta);

		/**
		* El otro caso es cuando los puntos ganados son igual o mayor al maximo para calculo
		* entonces los puntos ganados = al maximo de puntos
		*/
		$consulta = "
		UPDATE tmppunt as tp
		SET
			tp.GDia = tp.I * tp.F9
		WHERE
			tp.PDia >= tp.I
			AND tp.Sign = 1";
		$this->consulta($consulta);

		$consulta = "
		UPDATE tmppunt as tp
		SET
			tp.GMes = tp.I * tp.F9
		WHERE
			tp.PMes >= tp.I
			AND tp.Sign = 1";
		$this->consulta($consulta);

		//se calculan puntos negativos
		$this->mensaje("   Calcula puntos ganados de indicadores negativos...");
		$consulta = "
		UPDATE tmppunt as tp
		SET
			tp.GDia = tp.F9-((1-tp.PDia)*tp.F9)
		WHERE
			tp.Sign = -1";
		$this->consulta($consulta);

		$consulta = "
		UPDATE tmppunt as tp
		SET
			tp.GMes = tp.F9-((1-tp.PMes)*tp.F9)
		WHERE
			tp.Sign = -1";
		$this->consulta($consulta);

		//excepciones
		$consulta = "
		UPDATE tmppunt as tp
		SET
			tp.GDia = 0
		WHERE
			tp.SIgn = -1
			and tp.PDia > tp.I";
		$this->consulta($consulta);

		$consulta = "
		UPDATE tmppunt as tp
		SET
			tp.GMes = 0
		WHERE
			tp.SIgn = -1
			and tp.PMes > tp.I";
		$this->consulta($consulta);

		$consulta = "
		UPDATE tmppunt as tp
		SET
			tp.GDia = 0
		WHERE
			tp.SIgn = -1
			and tp.PDia < tp.D";
		$this->consulta($consulta);

		$consulta = "
		UPDATE tmppunt as tp
		SET
			tp.GMes = 0
		WHERE
			tp.SIgn = -1
			and tp.PMes < tp.D";
		$this->consulta($consulta);

		$consulta = "
		UPDATE tmppunt as tp
		SET
			tp.GDia = tp.F9-((tp.PDia-1)*tp.F9)
		WHERE
			tp.Sign = -1
			AND tp.PDia > 1";
		$this->consulta($consulta);

		$consulta = "
		UPDATE tmppunt as tp
		SET
			tp.GMes = tp.F9-((tp.PMes-1)*tp.F9)
		WHERE
			tp.Sign = -1
			AND tp.PMes > 1";
		$this->consulta($consulta);

		//actualizamos el tipo de dato en F15 y F16
		$consulta = "
		ALTER TABLE iphone
			CHANGE F15 F15 decimal(59,4),
			CHANGE F16 F16 decimal(59,4)";
		$this->consulta($consulta);

		//se inserta los resultados de calculo de puntos en iphone
		$consulta = "
		UPDATE iphone as ip,
		tmppunt as tp
		SET
			ip.F15 = tp.GDia,
			ip.F16 = tp.Gmes
		WHERE
			ip.Branch = tp.Branch
			AND ip.Id = tp.Id";
		$this->consulta($consulta);

		//si del calculo resultan puntos negativos se dejan en ceros
		$consulta = "
		UPDATE iphone as ip
		SET
			ip.F15 = 0
		WHERE
			ip.F15 < 0";
		$this->consulta($consulta);

		$consulta = "
		UPDATE iphone as ip
		SET
			ip.F16 = 0
		WHERE
			ip.F16 < 0";
		$this->consulta($consulta);

		//si del calculo resultan nullos se dejan en ceros
		$consulta = "
		UPDATE iphone as ip
		SET
			ip.F15 = 0
		WHERE
			ip.F15 is NULL
			and ip.F1 >= 0 or ip.F1 <= 0";
		$this->consulta($consulta);

		$consulta = "
		UPDATE iphone as ip
		SET
			ip.F16 = 0
		WHERE
			ip.F16 is NULL
			and ip.F1 >= 0 or ip.F1 <= 0";
		$this->consulta($consulta);

		//Redondea
		$consulta = "
		UPDATE iphone as ip
		SET
		ip.F15 = round(ip.F15),
		ip.F16 = round(ip.F16)";
		$this->consulta($consulta);

		$this->eliminaTabla("tmppunt");

	}

	/**
	 * Método que exporta la tabla que se le indica para dejarla en el directorio SM
	 *
	 * @param string $tabla
	 */
	protected function exportaDBU($tabla = null) {
		$this->mensaje("     Exporta dbu");
		$this->depurar(__METHOD__);

		$tabla = strtolower($tabla);
		if ($this->existeTabla(PREFIJO_SC . $this->client . '.' . $tabla)) {
			$this->mensaje('Exportando la tabla ' . $tabla . ' para el DBU...');

			$mysqldump = "mysqldump";
			if (SO == "WINDOWS")
				$mysqldump = "%mysqldump%";

			$this->decodificaDSN($this->dsn,
				$servidor, $usuario, $contrasena, $basedatos, $puerto);

			foreach ($this->sucursales as $sucursal => $parametros) {
				if (str_replace(" ", "_", strtolower($parametros['Made'])) == 'mercedes_benz') {
					$nombre_corto = strtoupper(str_replace(" ", "", substr($parametros["Made"], 0, 8)));
				} else {
					$nombre_corto = strtoupper(str_replace(" ", "_", $parametros["Made"]));
				}

				$directorio = RAIZ_SMIG . '/' . $nombre_corto . '/' . strtoupper($tabla);

				if(!$this->existeDirectorio($directorio))
					$this->creaDirectorio($directorio);

				// Exportar dump al directorio del fabricante
				$archivo = $directorio . '/' . $this->client . str_pad($sucursal, 2, "0", STR_PAD_LEFT) . ".sql.dump";
				$comando = "$mysqldump " . PREFIJO_SC . $this->client . " -h $servidor -P $puerto -u $usuario -p$contrasena --tables $tabla --compress --compatible=ansi --where=\"Branch = $sucursal \" > $archivo";
				$this->ejecutaComando($comando, true, true);

				// Copiar a directorio de año - mes
				$ano = date('Y', $this->ayer);
				$mes = date('m', $this->ayer);
				$directorio = $directorio . '/' . $ano . '/' . $mes;

				if(!$this->existeDirectorio($directorio))
					$this->creaDirectorio($directorio);

				$archivo_mes = $directorio . '/' . $this->client . str_pad($sucursal, 2, "0", STR_PAD_LEFT) . ".sql.dump";
				copy($archivo, $archivo_mes);
			}

			$this->mensaje();
		}

		return $this;
	}

	protected function exportaSQLite() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102ESQL', 'Exportacion de Data.sim']);
		$this->identificaCliente();

		$this
			->usarBaseDatos(PREFIJO_SC.$this->client, true)
			->mensaje("   Exportando bases de datos de SQLite...");

		if ($dsn = $this->dsn)
			$dsn = "dsn=$dsn";

		$php = "php";
		if (SO == "WINDOWS")
			$php = "%php53%";

		$depurar = null;
		if ($this->depurar)
			$depurar = "depurar";

		$this->mensaje("      $this->ejecutaComando($comando, true, true);.sim...");

		$comando = "$php -f ".dirname(__FILE__)."/mysql2sqlite.php -- $dsn basedatos=".PREFIJO_SC.$this->client." directorio=".SW_UPLOAD." archivo=Data limpiar $depurar";
		$this->ejecutaComando($comando, true, true);

		$this
			->mensaje()
			->mensaje("      Exportando iPhone.sim...");

		$comando = "$php -f ".dirname(__FILE__)."/mysql2sqlite.php -- $dsn basedatos=".PREFIJO_SC.$this->client." directorio=".SW_UPLOAD." archivo=iPhone tabla=iphone nombre_tabla=_{$this->client} limpiar $depurar";
		$this->ejecutaComando($comando, true, true);

		$this->guardaLog(['02x0102ESQL', 'Exportacion de Data.sim']);

		return $this;
	}

	protected function ejecutaPostTablero($postprocesos = "*" , $dataserver = null) {
		$this->depurar(__METHOD__."($postprocesos)");

		if ($this->postprocesos) {

			$this->guardaLog(['01x0102EPSP', 'Ejecuta Postablero']);

			if (!$this->cliente)
				$this->identificaCliente();

			if ($dsn = $this->dsn)
				$dsn = "dsn=$dsn";

			$depurar = null;
			if ($this->depurar)
				$depurar = "depurar";

			$DATABASE_SERVER 	= (is_null($dataserver)) ? DATABASE_SERVER : $dataserver;
			$mysql 				= (SO == "WINDOWS") ? "%mysql%" 		: "mysql" ;
			$mysqldump 			= (SO == "WINDOWS") ? "%mysqldump%"		: "mysqldump" ;
			$php 				= (SO == "WINDOWS") ? "%php53%" 		: "php" ;

			$this->decodificaDSN($this->dsn,$servidor, $usuario, $contrasena, $basedatos, $puerto);

			$procesos = [];
			$procesos["convierte_gamonth"] = array("Convierte GAMONTH", "dsn" => "mysql://mig$usuario:$contrasena@$DATABASE_SERVER:5123", "simserver" => 'SMig', "total_meses" => "1", "archivo" => "{$this->client}*.sql.dump", "ignorar", $depurar);

			$procesos["calcula_incidencias"] = array("Incidencias Avanzadas", "dsn" => "mysql://mig$usuario:$contrasena@$DATABASE_SERVER:5123", "directorio" => SW_UPLOAD, "prefijo" => PREFIJO_SIM, "client" => $this->client, "no_convertir", $depurar);

			$procesos["completa_gamonth"] = array("Completa GAmonth", "dsn" => "mysql://mig$usuario:$contrasena@$DATABASE_SERVER:5123", "directorio" => SW_UPLOAD,  "prefijo" => PREFIJO_SIM ,"client" => $this->client, $depurar);

			$procesos["genera_types"] = array("Semaforo de Types", "dsn" => "mysql://mig$usuario:$contrasena@$DATABASE_SERVER:5123", "prefijo" => PREFIJO_SIM, "client" => $this->client, $depurar);

			$postprocesos = explode(",", $postprocesos);
			$this->depurar(print_r($postprocesos, true));
			$this->depurar(print_r($this->marcas, true));

			foreach ($procesos as $programa => $parametros)
				if ($postprocesos[0] == "*" || in_array($programa, $postprocesos)) {
					$mensaje = $parametros[0];

					unset($parametros[0]);

					$str_comando = null;
					foreach ($parametros as $llave => $parametro) {
						if (!is_numeric($llave))
							$str_comando .= $llave."=";

						$str_comando .= $parametro." ";

					}

					$this->mensaje("      $mensaje...\n");

					$this->guardaLog(['01x0107EPSP', strtolower($mensaje)]);

					switch($programa) {
						case 'convierte_gamonth':
							foreach($this->marcas as $marca) {
								$fabricante = $marca['NombreCorto'];
								$comando = "$php -f ".dirname(__FILE__)."/$programa.php -- $str_comando fabricante={$fabricante}";
									if ($this->ejecutaComando($comando, false, true)) {
										$this->guardaLog(['02x0107EPSP', strtolower($mensaje)]);
									} else {
										$this->error('No se logro completar el postproceso ' . $programa, false);
										$this->guardaLog(['01x0101POST', 'No se logro completar el postproceso ' . $programa]);
										$this->guardaLog(['01x0107EPSP', strtolower($mensaje)]);
									}
							}
							break;

						default:
							$comando = "$php -f ".dirname(__FILE__)."/$programa.php -- $str_comando";
								if ($this->ejecutaComando($comando, false, true)) {
									$this->guardaLog(['02x0107EPSP', strtolower($mensaje)]);
								} else {
									$this->error('No se logro completar el postproceso ' . $programa, false);
									$this->guardaLog(['01x0101POST', 'No se logro completar el postproceso ' . $programa]);
								}
							break;
					}

					$this->mensaje();

				}

			$this->mensaje();

		}

		$this->guardaLog(['02x0102EPSP', 'Ejecuta Postablero']);

		return $this;
	}

	protected function ejecutaPostProcesos() {
		$this->depurar(__METHOD__);
		$this->guardaLog(['01x0102EPSP', 'Ejecuta Postprocesos']);

		$DATABASE_SERVER 	= (is_null($dataserver)) ? DATABASE_SERVER : $dataserver;
		$mysql 				= (SO == "WINDOWS") ? "%mysql%" 	: "mysql" ;
		$mysqldump 			= (SO == "WINDOWS") ? "%mysqldump%"	: "mysqldump" ;
		$php 				= (SO == "WINDOWS") ? "%php53%" 	: "php" ;
		$sqlite 			= (SO == "WINDOWS") ? "%sqlite%"	: "sqlite" ;


		if ($this->postprocesos) {
			if (!$this->existeDirectorio(LOG . DIRECTORY_SEPARATOR . $this->client ))
				$this->creaDirectorio(LOG . DIRECTORY_SEPARATOR . $this->client );

			$this->decodificaDSN($this->dsn,$servidor, $usuario, $contrasena, $basedatos, $puerto);

			$consulta = "
			SELECT a.Aplicacion,a.Comando , a.Registro
			FROM app_prdlog.postprocesos as a
			INNER JOIN app_prdlog.postprocesos_clientes as c
			ON a.Aplicacion = c.Aplicacion
			AND a.Activa = 1
			AND c.Activa = 1
			AND c.CLIENT IN (0,".$this->client.")";

			if ($this->postproceso)
				$consulta .= " AND c.Aplicacion LIKE \"".$this->postproceso."\"";

			$consulta .= " ORDER BY c.Orden, c.Momento ";

			if ($resultado = $this->consulta($consulta)) {
				while ($fila = $resultado->fetch_assoc()) {
					$aplicacion = ($fila["Aplicacion"]);
					$comando = ($fila["Comando"]);
					$registro = ($fila["Registro"]);
					$this->guardaLog(['01x0107EPSP', strtolower($aplicacion)]);

					$comando = str_replace("%USUARIO%", "mig$usuario", $comando);
					$comando = str_replace("%CONTRASENA%", "$contrasena", $comando);
					$comando = str_replace("%CLIENT%", $this->client, $comando);
					$comando = str_replace("%LOG%", RAIZ_L, $comando);
					$comando = str_replace("%PHP%", $php, $comando);
					$comando = str_replace("%DIR%", dirname(__FILE__) , $comando);
					$comando = str_replace("%MYSQLDUMP%", $mysqldump, $comando);
					$comando = str_replace("%MYSQL%", $mysql, $comando);
					$comando = str_replace("%SQLITE%", $sqlite, $comando);
					$comando = str_replace("%PREFIJO_SIM%", PREFIJO_SIM, $comando);
					$comando = str_replace("%SW_UPLOAD%", SW_UPLOAD, $comando);
					$comando = str_replace("%DATABASE_SERVER%", $DATABASE_SERVER, $comando);
					$registro = str_replace("%CLIENT%", $this->client, $registro);
					$registro = str_replace("%LOG%", RAIZ_L, $registro);

					$comando = $comando . " > " . $registro ;

					$this->mensaje("   Ejecutando '$aplicacion'... \n ");
					if ($this->ejecutaComando($comando,false)) {
						$this->guardaLog(['02x0107EPSP', strtolower($aplicacion)]);
					}
				}
			}

			$resultado->close();

		}

		$this->guardaLog(['02x0102EPSP', 'Ejecuta Postprocesos']);

		return $this;
	}

	protected function transfiereSQLite($demo = null, $dataserver = null , $test = false) {
		$this->depurar(__METHOD__);
		$DATABASE_SERVER 	= (is_null($dataserver)) ? DATABASE_SERVER : $dataserver;
		$ftp				= ($test == false) ? URI_FTP_MIGRACION : URI_FTP_TEST_MIG;

		$this->mensaje("Transfiriendo a $DATABASE_SERVER...");

		if ($this->postprocesos)
			$this->guardaLog(['01x010380ZI', 'Batch 80-ZIP']);

		if ($this->transferir) {
			$this->identificaCliente();

			$client = $this->client;
			$directorio = SW_UPLOAD;

			if ($demo) {
				$client = $demo;
				$directorio = SW_UPLOAD . "/$demo";
			}

			$this
				->usarBaseDatos(PREFIJO_SC.$client, true)
				->mensaje("   Transfiriendo bases de datos de SQLite...");

			$this->mensaje("      Transfiriendo Data.sim...");

			$this->guardaLog(['01x0102TSQL', 'Transfiriendo Data.sim...']);
			if($this->transfiereArchivo($directorio."/Data.sim", $ftp.$client)){
				$this->guardaLog(['02x0102TSQL', 'Transfiriendo Data.sim...']);
			}

			$this->mensaje("      Transfiriendo iPhone.sim...");

			$this->guardaLog(['01x0102TSQL', 'Transfiriendo iPhone.sim...']);
			if($this->transfiereArchivo($directorio."/iPhone.sim",$ftp.$client)){
				$this->guardaLog(['02x0102TSQL', 'Transfiriendo iPhone.sim...']);
			}

			if (!$this->desarrollo) {
				$this->comprimirArchivo($directorio,'Data.sim', $client.".zip");
				if($this->transfiereArchivo($directorio."/".$client.".zip",$ftp.$client))
					$this->mensaje("      Transfiriendo ".$client.".zip");
			}

			$this->mensaje();
		}

		if ($this->postprocesos)
			$this->guardaLog(['02x010380ZI', 'Batch 80-ZIP']);

		return $this;
	}

    /**
     * Método que transfiere archivos al Delivery
     */
    protected function transfiereDelivery() {
		$this->depurar(__METHOD__);

		if ($this->transferir) {
			$this->identificaCliente();
			$this->mensaje('      Transfiriendo Data.sim y Iphone.sim a Delivery del dia...');

			if ($this->client) {
				$credencialesFTP = array();

				// Se definen las credenciales dependiendo del entorno
				switch (PREFIJO_SIM) {
				//	case 'mig_':
				//		$credencialesFTP['uriFtp'] = URI_FTP_DELIVERY_MIGRACION;
				//	break;
					case 'sim_':
				//		$credencialesFTP['uriFtp'] = URI_FTP_DELIVERY;
						$credencialesFTP['uriFtp'] = URI_FTP_DELIVERY_MIGRACION;
					break;
				}

				if (!empty($credencialesFTP)) {
					// Se genera el directorio en Delivery, que corresponde a la carpeta del día de hoy
					$directorioDelivery = date('j', $this->hoy) . '/' . $this->client;

	                // Arreglo de archivos que se subirán al delivery. Agregar o quitar en caso necesario
	                $archivos = array('Data.sim', 'iPhone.sim');

	                foreach ($archivos as $archivo) {
	                    $this->mensaje('      Transfiriendo ' . $archivo . ' a delivery...');

	                    if ($this->existeArchivo($archivo, SW_UPLOAD . '/')) {
	                        $this->transfiereArchivo(
	                            SW_UPLOAD . '/' . $archivo,
	                            $credencialesFTP['uriFtp'] . $directorioDelivery
							);
	                    } else {
	                        $this->error('      No existe el archivo ' . $archivo, false);
	                    }
	                }
		        } else {
		            $this->error('      No se identificaron credenciales para Delivery', false);
		        }
			}

		}

		return $this;
	}

	protected function procesoExterno($momento, $funcion, $tipo, $filtros = null) {
		$this->depurar(__METHOD__);

		$this->identificaCliente();

		$client = is_null($filtros) ? "AND client = {$this->client}" : $filtros ;

		$directorio=RAIZ_E."/"."crm_simetrical"."/";
		if (!$this->existeTabla("crm_simetrical.procesos_externos")) {
			if ($this->existeArchivo("procesos_externos.sql.dump", $directorio)) {
				$mysqldump = "mysqldump";
				$mysql = "mysql";

			if (SO == "WINDOWS") {
				$mysqldump = "%mysqldump%";
				$mysql = "%mysql%";
			}
				$this->decodificaDSN($this->dsn,
					$servidor, $usuario, $contrasena, $basedatos, $puerto);

				$this->mensaje("   Importando tabla 'procesos'...");
				$directorio="crm_simetrical";

				$comando = "$mysql $directorio -h $servidor -P $puerto -u $usuario -p$contrasena --force  < ".RAIZ_E."/"."crm_simetrical"."/procesos_externos.sql.dump";
				$this->ejecutaComando($comando);
			}
		}

		if ($this->client) {
			if ($this->existeTabla("crm_simetrical.procesos_externos")) {

				$this->mensaje();

				if ($dsn = $this->dsn)
				$dsn = "dsn=$dsn";

				$php = "php";
				if (SO == "WINDOWS")
					$php = "%php53%";

				$depurar = null;
				if ($this->depurar)
					$depurar = "depurar";

				$this->decodificaDSN($this->dsn,$servidor, $usuario, $contrasena, $basedatos, $puerto);

				$this->procesos = array();

				$consulta = "
					SELECT
						proceso
					FROM
						crm_simetrical.procesos_externos
					WHERE
						funcion='$funcion'
					AND
						momento='$momento'
					AND
						tipo= $tipo

				   $client

					AND
						Estado= 1  ORDER BY orden ASC ";

				if ($resultado = $this->consulta($consulta)) {
					$this->mensaje("   Ejecutando  ".strtoupper($momento)."--".strtoupper($funcion)." Client '".$this->client."'...");

					while ($fila = $resultado->fetch_assoc()) {
						$this->procesos[$fila["proceso"]] = $fila;
						$reporte=$fila["proceso"];

						if ($this->existeArchivo($reporte.".php", dirname(__FILE__).DIRECTORY_SEPARATOR)) {

							$this->mensaje("      Ejecutando reporte '".strtoupper($reporte)."' ...");
							$this->guardaLog(['01x0102PREX', "Proceso Externo $reporte"]);
							$this->mensaje();

							$comando="$php -f ".dirname(__FILE__).DIRECTORY_SEPARATOR.$reporte.".php -- dsn=mysql://$usuario:$contrasena@$servidor:$puerto client=".$this->client;

							if (!$this->ejecutaComando($comando, true, true)) {
								$this->mensaje("No se logro completar el Procesos Externo ".$fila["proceso"],false);
								$this->guardaLog(['01x0101ECMD', 'No se logro completar el Procesos Externo ' . $fila["proceso"]]);
							} else {
								$this->guardaLog(['02x0102PREX', "Proceso Externo $reporte"]);
							}

						} else {
							$this->mensaje("   El reporte ".strtoupper($reporte) ." No existe ...");
							$this->guardaLog(['01x0101ARCH', 'El reporte ' . strtoupper($reporte) . ' No existe ...', 'error_']);
						}

						$this->mensaje();
					}

					$resultado->close();
					$this->depurar(print_r($this->procesos, true));

				}else {
					$this->mensaje("Cliente '".$this->client."' No tiene proceso ".strtoupper($momento)."-".strtoupper($funcion)."...");
				}

			} else {
				$this->mensaje("Cliente '".$this->client."' No tiene proceso ".strtoupper($momento)."-".strtoupper($funcion)."...");
			}

		}

		$this->mensaje();
		return $this;
	}

	protected function lanzadorComandos($tablaProceso, $nombreProceso, $tipo) {
		$this->depurar(__METHOD__);

		$this->identificaCliente();

		$mysqldump = "mysqldump";
		$mysql = "mysql";

		if (SO == "WINDOWS") {
			$mysqldump = "%mysqldump%";
			$mysql = "%mysql%";
		}

		if ($dsn = $this->dsn) {
			$dsn = "dsn=$dsn";

			$php = "php";
			if (SO == "WINDOWS")
				$php = "%php53%";

			$depurar = null;
			if ($this->depurar)
				$depurar = "depurar";
		}

		$this->decodificaDSN($this->dsn,$servidor, $usuario, $contrasena, $basedatos, $puerto);

		if ($this->client) {

			if ($tablaProceso == true) {

				$directorio=RAIZ_E."/"."crm_simetrical"."/";

				if (!$this->existeTabla("crm_simetrical.procesos_externos")) {
					if ($this->existeArchivo("procesos_externos.sql.dump",$directorio)) {

						$this->mensaje("         Importando tabla 'procesos'...");
						$basedatos="crm_simetrical";

						$comando = "$mysql $basedatos -h $servidor -P $puerto -u $usuario -p$contrasena --force  < ".RAIZ_E."/"."crm_simetrical"."/procesos_externos.sql.dump";
						$this->ejecutaComando($comando);

					} else {
						$this->mensaje("         No hay datos a importar...");
					}
				}

				if ($this->existeTabla("crm_simetrical.procesos_externos")) {
						$this->procesos = array();

						$consulta = "
							SELECT
								proceso
							FROM
								crm_simetrical.procesos_externos
							WHERE
								tipo=$tipo
							AND
								client = {$this->client}
							ORDER BY orden ASC ";

						if ($resultado = $this->consulta($consulta)) {
							while ($fila = $resultado->fetch_assoc()) {
								$this->procesos[$fila["proceso"]] = $fila;
								$reporte=$fila["proceso"];

								if ($this->existeArchivo($reporte.".php", dirname(__FILE__).DIRECTORY_SEPARATOR )) {
									$this->mensaje("   Ejecutando reporte ".strtoupper($reporte)." ...");
									$this->mensaje();

									$comando="$php -f ".dirname(__FILE__).DIRECTORY_SEPARATOR.$reporte.".php -- dsn=mysql://$usuario:$contrasena@$servidor:$puerto client=".$this->client;

									if (!$this->ejecutaComando($comando, false, true)) {
										$this->error("No se logro completar el postproceso ".$fila["proceso"]."",false);
										$this->guardaLog(['01x0101ECMD', 'No se logro completar el postproceso ' . $fila['proceso'], 'error_']);
									}
								} else {
									$this->mensaje("   El reporte ".strtoupper($reporte) .".php No existe ...");
								}
								$this->mensaje();
							}
							$resultado->close();
						}
				} else {
					$this->mensaje("         No existe la tabla 'procesos'...");
				}
			}

			if ($nombreProceso == true) {
				$this->mensaje("      Lanzador de Scripts");
				$this->mensaje();

				if ($this->existeArchivo($nombreProceso.".php", dirname(__FILE__).DIRECTORY_SEPARATOR)){
					$this->mensaje("         Ejecutando reporte ".strtoupper($nombreProceso)." ...");
					$this->mensaje();

					$comando="$php -f ".dirname(__FILE__).DIRECTORY_SEPARATOR.$nombreProceso.".php -- dsn=mysql://$usuario:$contrasena@$servidor:$puerto client=".$this->client;
					if (!$this->ejecutaComando($comando, false, true)) {
						$this->error("         No se logro lanzar el script ",false);
						$this->guardaLog(['01x0101ECMD', 'No se logro lanzar el script ' . $nombreProceso, 'error_']);
					}

				} else {
					$this->mensaje("   El reporte ".strtoupper($reporte) ." No existe ...");
				}
			}

		}

		$this->mensaje();
		return $this;
	}

	protected function tablero() {
		$this->depurar(__METHOD__);

		$this->guardaLog(['01x010370SC', 'Batch 70-SC']);

		$fabricante = str_replace(" ", "_", strtolower($this->fabricante));

			$this->mensaje("Generando los datos para el tablero de control...");
			$this
				->procesoExterno('antes','tablero','1')
				->preparaTablas()
				->limpiaDatosOtrosClientes()
				->verificaIndicadores()
				->actualizaTypes()
				->creaDetalles()
				->exportaOperativos()
				->creaTablasCalculo()
				->preparaGestion()
				->indicadoresBalance()
				->generaIndicadores()
				->recalculaSaldos()
				->calculaindicadoresmixtos()
				->agrupaIndicadores()
				->indicadoresCalculados()
				->modificaGalib()
				->procesoExterno('despues','tablero','1');

		$this->guardaLog(['02x010370SC', 'Batch 70-SC']);

		return $this;
	}

	protected function exporta() {
		$this->depurar(__METHOD__);

		$fabricante = str_replace(" ", "_", strtolower($this->fabricante));

			$this->mensaje("Exporta los datos para el tablero de control...");
			$this
				->exportaGAMonth()
				->exportaGADay()
				->exportaGADate()
				->exportaGALib()
				->exportaGAPoints()
				->exportaIndMfr($fabricante)
				->exportaDB()
				->exportaIphone()
				->exportaSQLite();

		return $this;
	}

	protected function transfiere() {
		$this->depurar(__METHOD__);

		$fabricante = str_replace(" ", "_", strtolower($this->fabricante));

			$this->mensaje("Transfiere los datos para el tablero de control...");
			$this
				->transfiereSQLite()
				->transfiereDelivery();
			//	->DataSim2mysql();

		return $this;
	}

	protected function sincronizaData($bd = PREFIJO_SIM , $DATABASE_SERVER = null) {
		$this->depurar(__METHOD__);
		
		if ($this->sincronizar) {
			$DATABASE_SERVER = (is_null($DATABASE_SERVER)) ?  DATABASE_SERVER : $DATABASE_SERVER ;
	
			if ($this->rds) {
				$DATABASE_SERVER = $this->rds;
			}
			
			$this->mensaje("Exportando base de datos local a {$DATABASE_SERVER}...\n");
			$this->guardaLog(['01x0102SIRE', 'Sincronizacion a Base de datos Remota']);

			$this->identificaCliente();

			$this->usarBaseDatos($bd.$this->client , true);
			$this->limpiabd();

			$mysql 		= (SO == "WINDOWS") ? "%mysql%" 		: "mysql" ;
			$mysqldump 	= (SO == "WINDOWS") ? "%mysqldump%"		: "mysqldump" ;
			$php 		= (SO == "WINDOWS") ? "%php53%" 		: "php" ;
			$depurar 	= ($this->depurar)  ? "depurar" 		: null ;
			$dsn  		= ($this->dsn) ? "dsn=$dsn" : null ;

			$this->decodificaDSN($this->dsn,$servidor, $usuario, $contrasena, $basedatos, $puerto);

			$comando = "$mysql -h ".$DATABASE_SERVER." -P 5123 -u mig$usuario -p$contrasena -e\"CREATE DATABASE IF NOT EXISTS ".PREFIJO_SIM.$this->client."\" ";
			$this->ejecutaComando($comando);

			$this
				->mensaje("   Exportando a basedatos remota...")
				->mensaje();

			if (!$this->existeDirectorio(TEMP."/data")) {
				$this->creaDirectorio(TEMP."/data");
			}

			//$tablas = $this->listaTablas(PREFIJO_SIM.$this->client);
			$tablas = $this->validaTablas();

			foreach ($tablas as $tabla) {
				$this->mensaje("      '$tabla'...");

				// Se valida si es la tabla TYPES para quitar la opción '--compatible', ya que al exportar
				// genera salidas inesperadas en el sql.dump cuando existen caracteres especiales
				$compatible = strtolower($tabla) == 'types' ? '' : '--compatible=ansi';

				$comando = "$mysqldump " . PREFIJO_SIM . $this->client . " -h $servidor -P $puerto -u $usuario -p$contrasena --compress $compatible --skip-triggers --tables $tabla";
				$comando = " $comando > ".TEMP."/data/$tabla.sql.dump";
				$this->ejecutaComando($comando,false);

				$comando = "$mysql -h ".$DATABASE_SERVER." -P 5123 -u mig$usuario -p$contrasena -e\"DROP TABLE IF EXISTS ".PREFIJO_SIM.$this->client.".$tabla \" ";
				$this->ejecutaComando($comando);

				$comando = "$mysql ".PREFIJO_SIM.$this->client." -h ".$DATABASE_SERVER." -P 5123 -u mig$usuario -p$contrasena --force < ".TEMP."/data/$tabla.sql.dump";
				$this->ejecutaComando($comando);
				$this->eliminaArchivo("$tabla",TEMP.'/data');
			}

			$this->creaIndices();
	
			$this->guardaLog(['02x0102SIRE', 'Sincronizacion a Base de datos Remota']);
		}

		return $this;
	}

	protected function creaIndices($DATABASE_SERVER = null) {
		$this->depurar(__METHOD__);

		$this->mensaje("\n      Generando Indices...");

		$DATABASE_SERVER = (is_null($DATABASE_SERVER)) ?  DATABASE_SERVER : $DATABASE_SERVER ;
		$mysql 			 = (SO == "WINDOWS") ? "%mysql%" 	: "mysql" ;
		$mysqldump 		 = (SO == "WINDOWS") ? "%mysqldump%"	: "mysqldump" ;

		$this->decodificaDSN($this->dsn,$servidor, $usuario, $contrasena, $basedatos, $puerto);


		$this->indices["serpub"] = array("idName" => "Vin_", "columnsNames" => "Vin");
		$this->indices["vtanue"] = array("idName" => "VINFechaFactura", "columnsNames" => "Vin,FechaFactura");
		$this->indices["vtanue"] = array("idName" => "BranchFechaFacturaRFC", "columnsNames" => "Branch,FechaFactura,RFC");
		$this->indices["vtanue"] = array("idName" => "FechaFactura", "columnsNames" => "FechaFactura");
		$this->indices["oportunidades"] = array("idName" => "Mes_ConversionExcepcionLlaveComparacion", "columnsNames" => "Mes_,Conversion,Excepcion,Llave,Comparacion");
		$this->indices["oportunidades"] = array("idName" => "LlaveExcepcionMes_Estatus", "columnsNames" => "Llave,Excepcion,Mes_,Estatus");

		foreach ($this->indices as $campo => $parametros){
			$this->mensaje("         '$campo'...");
			$comando = "$mysql ".PREFIJO_SIM.$this->client." -h ".$DATABASE_SERVER." -P 5123 -u mig$usuario -p$contrasena -e\"";
			$comando .= "ALTER TABLE $campo ADD INDEX ".$parametros["idName"]."(".$parametros["columnsNames"].")"."\"";
			$this->ejecutaComando($comando, false);
		}


		return $this;
	}

	protected function validaTablas($respalda = false) {
		$this->depurar(__METHOD__);

		$this->mensaje("Validando Tablas a sincronizar...");

		$this->identificaCliente();

		$this->usarBaseDatos(PREFIJO_SIM.$this->client,true);

		$consulta = "SELECT DISTINCT LOWER(NAME) FROM tablas";
		$resultado = $this->consulta($consulta);

		$tablasbd = array();
		foreach ($resultado as $keyId => $tablas) {
			foreach ($tablas as $keyId => $tabla) {
				if ($this->existeTabla($tabla)) {
					array_push($tablasbd, $tabla);
				}
			}
		}

		if ($respalda == false)
			$tabla = 'tablas';

		/*if ($this->client == 156) {
			$demo = "'mac','refpot','seroep'";
		}*/

		$tablasConfig = array(
            'chart',
            'galib',
            'types',
            'gestion',
            'docs',
            'galabels',
            'mac',
            'objetivo',
            'docs',
            'fabrica',
            'gaindexd',
            'inderror',
            'param',
            'puntos',
            'schema',
            'xception',
            'calendar',
            $tabla
        );

		$this->tablasSinc =	array_merge($tablasbd, $tablasConfig);
		$this->depurar(print_r($this->tablasSinc, true));

		return $this->tablasSinc;
	}

	/**
	* Funcion Temporal para levantar el postproceso hacia database
	* generar la conversion de Data.sim a MySQL
	*/
	protected function solicitaProceso($proceso) {
		$php = "php";
		if (SO == "WINDOWS")
			$php = "%php53%";

		if ($this->transferir) {
			$DATABASE_SERVER = ($this->rds) ? $this->rds : 'database.simetrical.internal';
			$this->decodificaDSN($this->dsn, $servidor, $usuario, $contrasena, $basedatos, $puerto);
			$dsn = "mysql://mig$usuario:$contrasena@{$DATABASE_SERVER}:5123";

			$comando = "$php -f ".dirname(__FILE__)."/solicita_proceso.php -- dsn={$dsn} proceso={$proceso} client={$this->client}";
			$this->depurar($comando);
			passthru($comando, $resultado);

			if ($resultado != "0")
				$this->error("      No se logro solicitar el proceso: {$proceso}");
			sleep(1);
		}

		return $this;
	}

	protected function importaPlanta() {
		$this->depurar(__METHOD__);
		if ($dsn = $this->dsn)
				$dsn = "dsn=$dsn";

			$php = "php";
			if (SO == "WINDOWS")
				$php = "%php52%";

			$depurar = null;
			if ($this->depurar)
				$depurar = "depurar";

		$this->mensaje("Importando Datos ".$this->fabricante."...");

		$consulta = "DROP DATABASE IF EXISTS mfr_".$this->fabricante;
		$this->consulta($consulta);

		$consulta = "CREATE DATABASE mfr_".$this->fabricante;
		$this->consulta($consulta);

		$resultado = $this->listaArchivos(".DB",$this->directorio);

		foreach ($resultado as $indice => $tabla) {
			$this->mensaje("   $tabla...");

			$comando = "$php -f ".dirname(__FILE__)."/pdx2mysql.php -- $dsn directorio=".$this->directorio." basedatos=mfr_".$this->fabricante." archivo=$tabla limpiar";
			$this->ejecutaComando($comando, true, true);

		}

		$this->mensaje();

		return $this;
	}

	protected function respalda($tablas = null, $directorio = null) {
		$this->depurar(__METHOD__);

		$this->identificaCliente();

        $mysqldump = (SO == "WINDOWS") ? '%mysqldump%' : 'mysqldump';
        $db = PREFIJO_SIM . $this->client;
        //$directorio=RAIZ_E . '/' . $this->client;
        $directorio = (is_null($directorio)) ? RAIZ_E . '/' . $this->client : $directorio ;

        if (!$this->existeDirectorio($directorio)) {
            $this->creaDirectorio($directorio);
        }

		$this->decodificaDSN($this->dsn, $servidor, $usuario, $contrasena, $basedatos, $puerto);

		$comando = "$mysqldump $db -h $servidor -P $puerto -u $usuario -p$contrasena --compress --skip-triggers";

		if ($tablas == null) {
			$this->guardaLog(['01x010390BK', 'Batch 90-BAK']);

			$this->limpiabd();
			$this->eliminaIndices();

			if ($this->client && $this->respaldar) {

				$this->mensaje("      Generando el DUMP...");
				$this->guardaLog(['01x0102RESP', 'Generando DUMP del cliente...']);

				if ($this->auto_tbl) {
					if ($this->eliminaTabla("tablas"))
						$this->mensaje("         Eliminando tabla 'tablas'...\n");
				}

				if (count($tablas = $this->listaTablas(array('excluir' => 'gestiont')))){
					$this->depurar(print_r($tablas, true));
					$this->mensaje("         Respaldando");

					foreach ($tablas as $tabla => $value) {
						if ($this->existeTabla($tabla)) {
							if ($this->ejecutaComando($comando." --tables $tabla > $directorio/$tabla.sql.dump", false)) {
								$this->mensaje("            '$tabla'");
							} else {
								$this->error('              No se respaldo la tabla ' . $tabla, false);
                                $this->guardaLog(['02x0102RESP', 'No se respaldo la  tabla ' . $tabla]);
							}
						}
					}
				}
				
				$this->guardaLog(['02x0102RESP', 'Generando DUMP del cliente...']);
				$this->mensaje();
			}

			$this->guardaLog(['02x010390BK', 'Batch 90-BAK']);
			if($this->existeArchivo($this->client.".sql.dump", RAIZ_E."/")){
				$this
					->eliminaArchivo($this->client.".sql.dump", RAIZ_E."/")
					->mensaje("      Eliminando archivo ".$this->client.".sql.dump")
					->mensaje();
			}

		} else {

			$listaTablas = ($tablas == '*') ? "galib,tablas,types,chart,lista,mac" : $tablas;
			$listaTablas = explode(",", $listaTablas);

			$this->mensaje("      Respaldando...");
			foreach ($listaTablas as $value => $tabla) {
				if ($this->existeTabla(PREFIJO_SIM.$this->client.".$tabla")) {
					if ($this->ejecutaComando($comando." --tables $tabla > $directorio/$tabla.sql.dump", false)) {
						$this->mensaje("        '$tabla'");
					}
				}
			}

			$this->mensaje();
		}

		return $this;
	}

	protected function transfiereLog() {
		$this->depurar(__METHOD__);

		$this->identificaCliente();

		if ($this->transferir) {
			$this
				->mensaje()
				->mensaje("      Transfiriendo Log...");

				if (SO == "LINUX") {
					if ($this->existeArchivo($this->client.".log",LOG."/Scorecard/")) {

						$directorioOrigen=LOG."/Scorecard/".$this->client.".log";
						$directorioDestino=RAIZ_L."/".$this->client;

						$dirLogSQLOrigen = TEMP."/SQLog/{$this->client}/{$this->client}-*.log";
						$comando="cp $directorioOrigen $directorioDestino";

						if (!$this->existeDirectorio($directorioDestino)){
							$this->creaDirectorio($directorioDestino);
						}

						$dirLogSQL = TEMP."/SQLog/{$this->client}";
						if ($this->existeDirectorio($dirLogSQL)) {
							$archivos = $this->listaArchivos(".log", $dirLogSQL);
							if (count($archivos) > 0) {
								$comandoSql = "cp $dirLogSQLOrigen $directorioDestino";
								$this->ejecutaComando($comandoSql);
							}
						}
						if ($this->ejecutaComando($comando)) {
							$this->mensaje("      	Log Transferido...");
							$this->guardaLog(['02x0102RESL', 'Log Transferido...']);
						} else {
							$this->mensaje("      	Error al Transferir el archivo Log...");
							$this->guardaLog(['02x0102RESL', 'Error al Transferir el archivo Log...']);
						}

						$comando = "aws s3 cp ".LOG."/Scorecard/ s3://dump.sim/Logs/ProcesamientoCliente/ --recursive --region us-east-1";
						$this->ejecutaComando($comando, false, true);
					} else {
						$this->mensaje("         No existe el archivo Log");
					}
				} else {
					$this->mensaje("      	Sistema operativo no soportado...");
				}

				$this->mensaje();
			}

		return $this;
	}

	protected function enviaDocs() {
		$this->depurar(__METHOD__);

		$horaValida =  date("H:i:s" ,strtotime("05:00:00"));
		$hora = date("H:i:s");

		$this
			->mensaje("\nValidando paquetes procesados por cliente...")
			->mensaje("  Hora: $hora");

		$paquetes_procesados_today = $this->listaArchivos(".zip", SW_TODAY) ;
		$paquetes_procesados_wkg   = $this->listaArchivos(".zip", SW_WORKING) ;
		$paquetes_procesados_wait  = $this->listaArchivos(".zip", SW_WAIT) ;

		$paquetes_procesados = array_merge ($paquetes_procesados_today, $paquetes_procesados_wkg, $paquetes_procesados_wait  );

		$this->mensaje("     Enviando DOC's de las siguientes sucursales...");

		foreach ($paquetes_procesados as $key => $paquete) {
			$branch = substr($paquete, -8 , 2);
			$this->mensaje("        Sucursal: ".$branch);

			$this->ejecutaComando("wget --no-check-certificate -q -O- http://doc.simetrical.com/".$this->client."_".$branch." > /mnt/efs/block/Logs/".$this->client."/docs_{$branch}.log" , false);
		}

		$this->mensaje();
		return $this;

	}

	protected function respaldaPlanta() {
		$this->depurar(__METHOD__);

			$this
				->usarBaseDatos("mfr_".$this->fabricante, true)
				->mensaje("Respaldando la base de datos...");

			$mysqldump = "mysqldump";
			if (SO == "WINDOWS")
				$mysqldump = "%mysqldump%";

			$directorio=RAIZ_E."/mfr_".$this->fabricante;

			if (!$this->existeDirectorio($directorio)){
				$this->creaDirectorio($directorio);
			}

			$this->decodificaDSN($this->dsn,
				$servidor, $usuario, $contrasena, $basedatos, $puerto);

			$comando = "$mysqldump mfr_".$this->fabricante." -h $servidor -P $puerto -u $usuario -p$contrasena --compress --compatible=ansi --skip-triggers";

			$consulta = "SHOW TABLES";

			$resultado = $this->consulta($consulta);

			foreach ($resultado as $indices => $tablas) {
					foreach ($tablas as $indice => $tabla) {
						if ($this->existeTabla($tabla)) {
							$this
								->mensaje("   '$tabla'...")
								->ejecutaComando($comando." --tables $tabla > ".RAIZ_E."/mfr_".$this->fabricante."/$tabla.sql.dump");
					}
				}
			}

		$this->mensaje();

		return $this;
	}

	protected function eliminaIndices() {
		$this->depurar(__METHOD__);

			$this
				->usarBaseDatos(PREFIJO_SIM.$this->client)
				->mensaje("Elimnando Indices de la base de datos ".PREFIJO_SIM.$this->client);

			$consulta = "
			select index_schema,
			       index_name,
			        table_name
			from information_schema.statistics
			where table_schema='".PREFIJO_SIM.$this->client."'
			AND index_name <> 'PRIMARY'
			group by index_schema,
			         index_name,
			         index_type,
			         non_unique,
			         table_name
			order by index_schema,
			         index_name";

			if($resultado = $this->consulta($consulta)){
				while ($fila = $resultado->fetch_assoc()){
					$indexName = ($fila["index_name"]);
					$tabla  = ($fila["table_name"]);

					$consulta = "DROP INDEX `".$indexName."` ON ".PREFIJO_SIM.$this->client.".".$tabla;
					$this->consulta($consulta);

					echo ".";
				}

			}

		$this->mensaje();

		return $this;
	}

	protected function terminaServidor() {
		$this->depurar(__METHOD__);

		if ($this->terminar) {

			if ($this->existeDirectorio(TEMP . '/GuardaLog/')) {
				$actualizaGuardaLog = 'aws s3 mv /tmp/GuardaLog s3://dump.sim/GuardaLog/ --recursive';
				$this->ejecutaComando($actualizaGuardaLog, false, true);
			}

			$this->guardarProcesamiento(); // guardamos la fecha y hora del ultimo procesamiento correcto

			$this->mensaje("Apagando el servidor...");

			if ($instancia = AWS_INSTANCE) {

				// lanzamos el apagado del servidor en segundo plano
				$comando = "/prg/scripts/prdserver_shutdown.sh {$instancia} 30 > /mnt/efs/block/Logs/{$this->client}/terminate.log &";

				if (!$this->ejecutaComando($comando, false, true)) {
					$this->mensaje("	No se logró apagar el servidor");
					$this->guardaLog(['01x0101ECMD', 'Error al apagar la instancia: ' . AWS_INSTANCE]);
				}
			}

			$this->mensaje();
		}

		return $this;
	}

	protected function calculaindicadoresmixtos() {
		$this->depurar(__METHOD__);
		/**
		*	Es decir aquellos indicadores que se sacan
		*	de la Balanza y aparecen como tipo B pero que en realidad son de Resultados
		*	Como Gastos de Ventas
		**/

		$this->guardaLog(['01x0102CIMX', 'Calculo de indicadores mixtos']);

		$this->mensaje("Calcula Indicadores Mixtos \n");

		$this->mensaje("   Recalculando Indicadores de Balance Mixtos");

		$this->eliminaTabla("indmixtosb");

		$consulta = " CREATE /* TEMPORARY */TABLE indmixtosb AS (
		SELECT
			gt.Valor
		FROM
			gestion AS gt
		INNER JOIN
			galib AS ga
		ON ga.Id = gt.Ind
			AND ga.Id LIKE 'I%'
			AND ga.Kind = 'B'
			AND ga.Mixtos IS NOT NULL
			AND gt.Saldo = gt.Valor)";
		$this->consulta($consulta);
		$this->mensaje("      ".$this->filas_afectadas." registros insertados");

		$this->mensaje("   Recalculando Indicadores de Balance Mixtos");

		$this->eliminaTabla("indmixtosr");

		$consulta = "CREATE /* TEMPORARY */ TABLE indmixtosr AS
		(SELECT
			gt.Client,
			gt.Branch,
			gt.Ind,
			gt.Ano,
			gt.Mes,
			gt.Dia,
			gt.Valor,
			gt.Saldo
		FROM
			gestion AS gt
		INNER JOIN
			galib AS ga
		ON ga.Id = gt.Ind
			AND ga.Id LIKE 'I%'
			AND ga.Kind = 'R')";
		$this->consulta($consulta);
		$this->mensaje("      ".$this->filas_afectadas." registros insertados");

		$this->consulta("ALTER TABLE indmixtosr ADD INDEX ind (Ind)");

		$this->mensaje("   Recalculando Indicadores de Resultados");

		$consulta = "UPDATE indmixtosr SET Saldo = Valor WHERE Dia = 1";
		$this->consulta($consulta);
		$this->mensaje("      ".$this->filas_afectadas." registros actualizados");

		$this->mensaje("   Reinsertando Registros");

		$consulta = "
		UPDATE gestion AS g,
			indmixtosr AS t
		SET
			g.Valor = t.Valor,
			g.Saldo = t.Saldo
		WHERE
			g.Branch = t.Branch
		AND g.Ind = t.Ind
		AND g.Ano = t.Ano
		AND g.Mes = t.Mes
		AND g.Dia = t.Dia";
		$this->consulta($consulta);
		$this->mensaje("      ".$this->filas_afectadas." registros actualizados");

		$this->mensaje("   Redondea valores de GESTION");

		$consulta = "
		UPDATE gestion AS ge,
			galib AS ga
		SET
			ge.Valor = 0
		WHERE ge.Ind = ga.Id
		AND ga.Unit = '$'
		AND ge.Valor < 0.1 AND ge.Valor > -0.1";
		$this->consulta($consulta);

		$consulta = "
		UPDATE gestion AS ge,
			galib AS ga
		SET
			ge.Saldo = 0
		WHERE ge.Ind = ga.Id
		AND ga.Unit = '$'
		AND ge.Saldo < 0.1 AND ge.Saldo > -0.1";
		$this->consulta($consulta);

		$consulta = "
		UPDATE
			gestion
		SET
			Valor = 0
		WHERE
			Valor < 0.009 AND Valor > -0.009";
		$this->consulta($consulta);

		$consulta = "
		UPDATE
			gestion
		SET
			Saldo = 0
		WHERE
			Saldo < 0.009 AND Saldo > -0.009";
		$this->consulta($consulta);

		$this->mensaje();

		$this->guardaLog(['02x0102CIMX', 'Calculo de indicadores mixtos']);

		return $this;
	}

	protected function modificaGalib()
	{
		$bd = PREFIJO_SC.$this->client;

		$this
			->usarBaseDatos($bd, true)
			->mensaje("   Modifica GALIB para visualizar tablas de personal activo...");

		if ($this->existeTabla("tablas")) {
			$tablas = "tablas";
		} elseif ($this->existeTabla("indice.auto-tbl")){
			$tablas = "indice.`auto-tbl`";
		}

		$filtros = array();
		foreach (range(1,5) as $filtro) {
			$filtros[] = "CampoFiltro{$filtro} = NULL";
			$filtros[] = "Filtro{$filtro} = NULL";
		}
		$filtros = (!empty($filtros)) ? implode(",", $filtros) : '';

		$consulta = "UPDATE
						{$tablas} AS t,
						galib AS g
					SET g.Reporte = CONCAT('DB',Id), g.CampoType = NULL, g.`Type` = NULL, {$filtros}
					WHERE (t.Name = g.Reporte)
					AND t.Tipo <> 'B'
					AND g.Kind = 'B'
					AND g.Id LIKE 'I%'
					AND g.CampoFecha LIKE 'Date%'
					AND g.Operacion = 'COUNT'";

		$this->consulta($consulta);

		return $this;
	}

	protected function limpiabd() {
		$this->depurar(__METHOD__);

		$this->mensaje("Depurando base de datos ");

		$bd = PREFIJO_SIM.$this->client;

		$this
			->usarBaseDatos($bd, true)
			->mensaje("\nLimpiando gestion...\n");

        // Se limpia la tabla gestión para que no contenga totales
        // Se elimina el branch 0
        $consultaGestion = '
            DELETE FROM gestion
            WHERE branch = 0;
        ';
        $this->consulta($consultaGestion);

        // Se eliminan los demás totales
        $consultaGestion = '
            DELETE FROM gestion
            WHERE branch >= 1999;
        ';
        $this->consulta($consultaGestion);

        $this->mensaje("Limpiando base de datos '$bd'...");
        $this->mensaje("   Eliminando tablas temporales...");

		$consulta = "
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = '$bd' AND (TABLE_NAME LIKE 'tmp%' OR TABLE_NAME LIKE 'temp%')";

		$resultado = $this->consulta($consulta);

		foreach ($resultado as $indices => $tablas) {
				foreach ($tablas as $indice => $tabla) {
					if ($this->existeTabla($tabla)) {
						$this->mensaje("      '$tabla'...");
						$this->consulta("DROP TABLE `$tabla`");
				}
			}
		}
        // Se agrega arreglo de tablas para la limpieza final
        $this->mensaje("   Eliminando tablas innecesarias...");
        $eliminarTablas = array(
            '14meses',
            '2y2meses_',
            '4meses',
            'amd',
            'ceros',
            'Galibtm2',
            'containr',
            'gestiont',
            'virtuals',
            'indmixtosb',
            'indmixtosr',
            'primeros',
            'answer',
            'paso',
            'ventanuevosmatriz',
            'cruceFS',
            'galib-bk',
            'galib_bk',
            'primeros',
        );

        foreach ($eliminarTablas as $tabla) {
            if ($this->existeTabla($tabla)) {
                $this->mensaje("      '$tabla'...");
                $this->consulta("DROP TABLE `$tabla`");
            }
        }

		$this->mensaje("   Actualizando tablas con espacio en blanco...");
		$consulta = "
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = '$bd' AND TABLE_NAME LIKE '% %'";

		$resultado = $this->consulta($consulta);

		foreach ($resultado as $indices => $tablas) {
				foreach ($tablas as $indice => $tabla) {
					if ($this->existeTabla($tabla)) {
						$this->mensaje("      '$tabla'...");
						$this->consulta("ALTER TABLE `$tabla` RENAME TO `".str_replace(' ', '', $tabla)."`");
				}
			}
		}

		$this->mensaje("   Actualizando tablas con guion alto...");
		$consulta = "
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = '$bd' AND TABLE_NAME LIKE '%-%'";

		$resultado = $this->consulta($consulta);

		foreach ($resultado as $indices => $tablas) {
				foreach ($tablas as $indice => $tabla) {
					$tablaNueva = str_replace('-', '_', $tabla) ;
					if ($this->existeTabla($tabla)) {
						$this->eliminaTabla($tablaNueva);
						$this->mensaje("      '$tabla'...");
						$this->consulta("ALTER TABLE `$tabla` RENAME TO `$tablaNueva`");
				}
			}
		}

		$this->mensaje("Limpieza de '$bd' concluida... \n");
		return $this;
	}

	protected function deshabilitakpi($kpis) {
		$this->depurar(__METHOD__);

		$this->mensaje("   Deshabilitando KPI duplicados...");

		$kpis = explode(",", $kpis);
		$kpis = str_replace(' ', '', $kpis);

		$this->depurar(print_r($kpis, true));

		$count = null;
		foreach ($kpis as $key => $kpi) {
			$this->depurar("   ".$kpi);
			$this->mensaje("      Validando kpi $kpi...");
			$consulta = "
			UPDATE ".PREFIJO_SIM.$this->client.".galib AS gb,
				    (SELECT  `No`  FROM ".PREFIJO_SIM.$this->client.".galib
				    WHERE Id = '$kpi' AND Reporte IS NULL
				    	AND Campofecha IS NULL
				    	ORDER BY `Estimatecalcvalue` desc LIMIT 1) AS gl2
				SET
				    gb.Estimatecalcvalue = '1'
				WHERE
				    gb.No = gl2.No";
			$this->consulta($consulta);

			if (!$this->filas_afectadas) {
				$continua = 1;
				$this->mensaje("         No es un registro vacio...");
			}

			if ($continua) {
				$this->mensaje("         Es un registro duplicado...");
				$consulta = "
				UPDATE ".PREFIJO_SIM.$this->client.".galib AS gb,
				    (SELECT
				        `No`
				    FROM
				        ".PREFIJO_SIM.$this->client.".galib
				    WHERE
				        Id = '$kpi'
				    ORDER BY `Estimatecalcvalue` desc
				    LIMIT 1) AS gl2
				SET
				    gb.Estimatecalcvalue = '1'
				WHERE
				    gb.No = gl2.No";
				$this->consulta($consulta);
			}

			$this->mensaje("            Kpi $kpi deshabilitando...");
		}

		return $this;
	}

	protected function guardarProcesamiento() {
	$this->depurar(__METHOD__);
	/*
	* Esta funcion actualiza el estatus de un prdserver cuando termina de procesar por completo todas las funciones
	*/
		if ($this->existeArchivo("version.txt",SIMETRICAL."/")) {
			sleep(10);
			$this->decodificaDSN($this->dsn,$servidor, $usuario, $contrasena, $basedatos, $puerto);
			$consulta = "INSERT INTO app_prdlog.procesamientos_finalizados  (cliente , numProcesamiento,  fecha , proceso , horaInicia , horaFinaliza, instancia )
							VALUES ('".$this->client."' , '".$this->numProcesamiento."' , '".date("Y-m-d")."' , 'tablero' , '".$this->horaIniciaProcesamiento."' ,'".date("H:i:s")."', '".AWS_INSTANCE."')";

			$tempDSN = "mysql://mig$usuario:$contrasena@".DATABASE_SERVER.":5123";

			if($this->consulta($consulta , $tempDSN))
				$this->mensaje("   Se ha actualizado como terminado este procesamiento");

			$this->mensaje();
		}

		return $this;

	}

	protected function comprimirArchivo($directorio = null , $archivo = null, $nombreComprimido = null) {
		$this->depurar(__METHOD__."$directorio,$archivo,$nombreComprimido");

		if($this->existeArchivo($archivo, $directorio."/")){
			$this->mensaje("         Creando archivo comprimido '$nombreComprimido'...");

			$zip = new ZipArchive;
			$zip->open("$directorio/$nombreComprimido", ZipArchive::CREATE);
			$zip->addFile("$directorio/$archivo","$archivo");

			$zip->close();
			$this->mensaje("            Archivo creado con exito...");

		}

		$this->mensaje();

		return $this;
	}

	protected function numeroProcesamiento() {
		$this->depurar(__METHOD__);

		if ($this->existeArchivo("version.txt",SIMETRICAL."/")) {
			$fecha = date('Y-m-d');
			$consulta = "SELECT IFNULL(MAX(numProcesamiento),0) as numProcesamientos FROM app_prdlog.procesamientos_finalizados where cliente = ".$this->client." and fecha = '$fecha' LIMIT 1";

			$this->decodificaDSN($this->dsn,$servidor, $usuario, $contrasena, $basedatos, $puerto);

			$tempDSN = "mysql://mig$usuario:$contrasena@".DATABASE_SERVER.":5123";

			if ($resultado = $this->consulta($consulta , $tempDSN )) {
				while ($fila = $resultado->fetch_assoc()) {
					$this->numProcesamiento = [$fila["numProcesamientos"]][0];
					$this->numProcesamiento++;
					$this->horaIniciaProcesamiento = date("H:i:s");
					$this->mensaje("Procesamiento : {$this->numProcesamiento}, Fecha : ".date('Y-m-d').", Hora de Inicio : {$this->horaIniciaProcesamiento}");
				}
			}

			$resultado->close();
		}

		return $this;
	}

	protected function generaDemo() {
		$this->depurar(__METHOD__);
		if ($this->existeTabla('indice.client_demo')) {

			$consulta = "SELECT client_demo FROM indice.client_demo WHERE client_original = {$this->client}";

			if ($resultado = $this->consulta($consulta)) {
				$demo = $resultado->fetch_all(MYSQLI_ASSOC);
				$resultado->close();
				$demo = $demo[0]['client_demo'];

				if ($demo) {
					$this->mensaje("Generando el cliente demo $demo ...");

					$this->mensaje("   sincronizando tablas del cliente demo $demo ...");


					if (!$this->existeDirectorio(SW_UPLOAD.'/'.$demo.'/'))
						$this->creaDirectorio(SW_UPLOAD.'/'.$demo.'/');

					$copyDataSimDemo = copy(SW_UPLOAD.'/Data.sim', SW_UPLOAD.'/'.$demo.'/Data.sim');

					if ($copyDataSimDemo) {
						$this->mensaje('	Data.sim copiado');
						$copyiPhoneSimDemo = copy(SW_UPLOAD.'/iPhone.sim', SW_UPLOAD.'/'.$demo.'/iPhone.sim');

						if ($copyiPhoneSimDemo) {
							$this->mensaje('	iPhone.sim copiado');

							$php = "php";
							if (SO == "WINDOWS")
								$php = "%php53%";

							$this->decodificaDSN($this->dsn, $servidor, $usuario, $contrasena, $basedatos, $puerto);

							$tempDSN = "mysql://mig$usuario:$contrasena@".DATABASE_SERVER.":5123";

							$this->mensaje('	Insertando datos ficticios...');
							$comando = "$php -f ".dirname(__FILE__)."/inserta_datos_ficticios_demo.php -- dsn=$tempDSN directorio=".SW_UPLOAD."/$demo prefijo=".PREFIJO_SIM." client=$demo origen={$this->client} no_mysql";

							if (!$this->ejecutaComando($comando, false, true)) {
								$this->mensaje('	No se logró completar el comando inserta_datos_ficticios_demo.php');
								return $this;
							}

							$this->mensaje("	Calculando incidencias en el Demo $demo...");
							$comando = "$php -f ".dirname(__FILE__)."/calcula_incidencias.php -- dsn=$tempDSN directorio=".SW_UPLOAD."/$demo prefijo=".PREFIJO_SIM." client=$demo origen={$this->client} no_convertir";

							if (!$this->ejecutaComando($comando, false, true)) {
								$this->mensaje('	No se logró completar el comando calcula_incidencias.php');
							}

							$this->mensaje("	Ejecutando completa_gamonth en el Demo $demo...");
							$comando = "$php -f ".dirname(__FILE__)."/completa_gamonth.php -- dsn=$tempDSN directorio=".SW_UPLOAD."/$demo prefijo=".PREFIJO_SIM." client=$demo";

							if (!$this->ejecutaComando($comando, false, true)) {
								$this->mensaje('	No se logró completar el comando completa_gamonth.php');
							}

							$this->mensaje("Transfiere los datos para el tablero de control del Demo $demo...");
							$this->transfiereSQLite($demo);

						} else {
							$this->mensaje('	No se logró copiar el iPhone.sim al cliente Demo');
						}
					} else {
						$this->mensaje('	No se logró copiar el Data.sim al cliente Demo');
					}

				}
			}

		} else {
			$this->mensaje();
			$this->mensaje("No existe la tabla 'indice.demo'...\n");
		}

		return $this;
	}

	protected function ultimosDump() {
		$this->depurar(__METHOD__);

		if (SO == 'LINUX') {
			if ($this->client && $this->respaldar) {
				$this->mensaje("Sincronizando de Dump a UltimoDump...");

				$directorioDump = RAIZ_E . '/' . $this->client . '/';
				$directorioUltimoDump = RAIZ_UDUMP.'/'.$this->client;

				if (!$this->existeDirectorio($directorioUltimoDump)) {
					$this->creaDirectorio($directorioUltimoDump);
				}

				$this->guardaLog(['01x0102RESP', 'Sincronizando a UltimoDump...']);

				$comando = "rsync -av $directorioDump $directorioUltimoDump";
				if (!$this->ejecutaComando($comando, false, true)) {
					$this->error('				No se completo la sincronizacion a UltimoDump', false);
					$this->guardaLog(['02x0102RESP', 'No se completo la sincronizacion a UltimoDump']);
				}

				$this->guardaLog(['02x010390BK', 'Batch 90-BAK']);
				$this->guardaLog(['02x0102RESP', 'Sincronizando a UltimoDump...']);
			}
		}
		
		$this->mensaje();
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
						->NuevosClientesRefvtaMac2char()
						->procesaPaquetes()
						->indicadoresContables()
						->tablero()
						->exporta()
						->sincronizaData()
						->ejecutaPostTablero()
						->transfiere()
						->enviaDocs()
						->generaDemo()
						->respalda()
						->ultimosDump()
						->ejecutaPostProcesos()
						->transfiereLog()
						->terminaServidor();

					break;
				case "limpia":
					$this->limpiaEspacio();

					break;
                case "reestructura":
                    $this->reestructuraTabla();

                    break;
				case "limpia2":
					$this->limpiaEspacio(true);

					break;
				case "limpiabd":
					$this->limpiabd();

					break;
				case "identifica":
					$this->identificaCliente();

					break;
				case "sincroniza":
					$this->sincronizaCliente();

					break;
				case "sincroniza2":
					$this->sincronizaCliente2();

					break;
				case "sinc-tablasconfig":
					$this->sincronizaTablasConfiguracion('*');

					break;
				case "sinc-galib":
					$this->sincronizaTablasConfiguracion('galib');

					break;
				case "sinc-tablas":
					$this->sincronizaTablasConfiguracion('tablas');

					break;
				case "sinc-types":
					$this->sincronizaTablasConfiguracion('types');

					break;
				case "sinc-chart":
					$this->sincronizaTablasConfiguracion('chart');

					break;
				case "sinc-lista":
					$this->sincronizaTablasConfiguracion('lista');

					break;
				case "sinc-mac":
					$this->sincronizaTablasConfiguracion('mac');

					break;
				case "respalda-tablasconfig":
					$this->respalda('*');

					break;
				case "respalda-galib":
					$this->respalda('galib');

					break;
				case "respalda-types":
					$this->respalda('types');

					break;
				case "respalda-chart":
					$this->respalda('chart');

					break;
				case "respalda-lista":
					$this->respalda('lista');

					break;
				case "respalda-mac":
					$this->respalda('mac');

					break;
				case "respalda-mac":
					$this->respalda('mac');

					break;
				case "respalda-tablas":
					$this->respalda('tablas');

					break;
				case "especificaciones":
					$this->copiaEspecificaciones();

					break;
				case "descarga":
					$this->descargaPaquetes();

					break;
				case "macrefvta":
					$this->NuevosClientesRefvtaMac2char();

					break;
				case "paquetes":
					$this->procesaPaquetes();

					break;

				case "descomprime":
					$this->descomprimePaquete();

					break;
				case "transforma":
					$this->transformaTablas();

					break;
				case "importa":
					$this->importaTablas();

					break;
				case "consolida":
					$this
						->procesoExterno('antes','consolida','1')
						->consolidaTablas()
						->procesoExterno('despues','consolida','1');

					break;
				case "mac":
					$this
						->procesoExterno('antes','mac','1')
						->formateaMAC()
						->actualizaMAC()
						->procesoExterno('despues','mac','1');

					break;
				case "tablas":
					$this
						->procesoExterno('antes','tablas','1')
						->actualizaTablas()
						->procesoExterno('despues','tablas','1');

					break;
				case "contables":
					$this->indicadoresContables();

					break;
				case "refvta":
					$this
						->procesoExterno('antes','tablero','1');
					break;

				case "ptablas":
					$this->preparaTablas();

					break;
				case "tablero":
					$this->tablero();

					break;
				case "parametro":
					$this->creaTablasParametro();

					break;
				case "verifica":
					$this->verificaIndicadores();

					break;
				case "limpiaotros":
					$this->limpiaDatosOtrosClientes();

					break;
				case "detalles":
					$this->creaDetalles();

					break;
				case "plantas":
					$this->generaPlanta();

					break;
				case "operativos":
					$this->exportaOperativos();

					break;
				case "prepara":
					$this->creaTablasCalculo();

					break;
				case "gestion":
					$this->preparaGestion();

					break;
				case "indicadores":
					$this->generaIndicadores();

					break;
				case "saldos":
					$this->recalculaSaldos();

					break;
				case "agrupa":
					$this->agrupaIndicadores();

					break;
				case "calculados":
					$this->indicadoresCalculados();

					break;
				case "indmixtos":
					$this->calculaindicadoresmixtos();

					break;
				case "gamonth":
					$this->exportaGAMonth();

					break;
				case "gaday":
					$this->exportaGADay();

					break;
				case "gadate":
					$this->exportaGADate();

					break;
				case "galib":
					$this->exportaGALib();

					break;
				case "indmfr":
					$this->exportaIndMfr($fabricante);

					break;
				case "asesores":
					$this->creaTablaAsesoresVendedores();

					break;
				case "gapoints":
					$this->exportaGAPoints();

					break;
				case "iphone":
					$this->exportaIphone();

					break;
				case "exporta":
					$this->exportaSQLite();

					break;
				case "virtuals":
					$this->preparavirtuals();

					break;
				case "postprocesos":
					$this->ejecutaPostProcesos();

					break;
				case "postablero":
					$this->ejecutaPostTablero();

					break;
				case "convgamonth":
						$this->ejecutaPostTablero("convierte_gamonth");

						break;
				case "advxcept":
					$this->ejecutaPostTablero("calcula_incidencias");

					break;
				case "cgamonth":
					$this->ejecutaPostTablero("completa_gamonth");

					break;
				case "stypes":
					$this->ejecutaPostTablero("genera_types");

					break;
				case "atypes":
					$this->actualizaTypes();

					break;
				case "transfiere":
					$this->transfiereSQLite();

					break;
				case "delivery":
					$this->transfiereDelivery();

				break;
				case "respalda":
					$this->respalda();

					break;
				case "tsflog":
					$this->transfiereLog();

					break;
				case "rplanta":
					$this->respaldaPlanta();

					break;
				case "iplanta":
					$this->importaPlanta();

					break;
				case "termina":
					$this->terminaServidor();

					break;
				case "comprimir":
					$this->comprimirArchivo();

					break;
				case "exportatablero":
					$this->exporta();

					break;
				case "transfieretablero":
					$this->transfiere();

					break;
				case "sincronizadata":
					$this->sincronizaData();

					break;
				case "guarda":
					$this->guardarProcesamiento();

					break;
				case "demo":
					$this->generaDemo();

					break;
				case "ultimodump":
					$this->ultimosDump();

					break;
				case "limpiaindice":
					$this->eliminaIndices();

					break;
				case "indices":
					$this->creaIndices();

					break;
				case "vtablas":
					$this->validaTablas();

					break;
				case "vsinc":
					$this->validaSincronizacion();

					break;
				case "kpibulk":
					$this->kpibulk();

					break;
				case "docs":
					$this->enviaDocs();

					break;
				default :
					$this->error("No existe un controlador para el proceso '$proceso'");
			}

		return $this;
	}
}

if (!CLASE_ABSTRACTA)
	Scorecard::singleton()->ejecutar();

?>